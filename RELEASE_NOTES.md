# Release Notes — NextGen Metadata Framework (Metaflow)

Semantic versioning: `MAJOR.MINOR.PATCH`. `pyproject.toml`'s `version` field is a build stamp
(UTC epoch-millis, rewritten by `scripts/bump_and_build.py` on every wheel build) and is
deliberately **not** the semantic version — that lives here and in `enhancement_logs/`.

---

## Source control — initial GitHub publish — 2026-08-30

Scope: repository plumbing only. **No framework, pipeline, app or control-table behaviour changed.**

The project is now tracked in git and published to
`github.com/Madhan-RAGHU/NextGen_Metadata_Framework` (private) on branch `main`,
which local `main` tracks. 984 files / 8.4 MB across three commits.

### 🔒 Security

**Databricks PAT redacted before the first commit.** A live-format token
(`dapi…`, 32 hex) had been pasted where a *profile name* was expected in
`metaflow_testing/TESTING_PLAN.md` §0 and `metaflow_testing/TESTING_STATUS.md` §0.
Both now read `` `<redacted-profile>` ``. The token never entered git history.
It should still be rotated in the workspace — it predates this commit and may
survive in local backups or shell history.

### 🔧 Changed

**`.gitignore`** gained two entries:

| Entry | Reason |
|---|---|
| `databricks-app/web/nul.css` | `nul` is a Windows reserved device name — git cannot index the file at all (`error: unable to index file`), which aborted staging outright. A 129-line orphan CSS bundle, referenced by nothing; left untouched on disk. |
| `.pytest_cache/` | Local test-run cache, not source. |

**Known gitignore quirk (not fixed here).** `dist/` excludes the directory, so the
later `!databricks-app/web/dist/**` and `!databricks-app/static/**` negations cannot
re-include anything — git will not re-include a file whose parent directory is
excluded. Built app assets under those paths are therefore **not** tracked. If the
deployed app needs them in the repo, the ignore rule must be narrowed (e.g. `/dist/`)
rather than negated.

### 📝 Notes

- Remote `main` already held one unrelated commit (`6140f3f Create test`, a blank
  placeholder). It was merged with `--allow-unrelated-histories` and the placeholder
  removed in a follow-up commit, so nothing on GitHub was force-discarded.
- Two remotes — `metaflow` and `metaflow_v2` — point at the *same* URL, and there is
  no `origin`. `main` tracks `metaflow`. Worth pruning the duplicate.
- Commit identity is repo-local: `Madhan-RAGHU <madhan@nrmanalytix.com>`.

---

## Onboarding App v1.6.0 + Documentation wiki — 2026-08-30

Scope: the **Databricks App** (`databricks-app/`) and the **documentation tree** (`docs/`). No
framework, pipeline or control-table behaviour changed.

Adds custom storage paths with server-side validation, a dynamic template directory, an attribute
inspector covering every attribute, and rebuilds the documentation as a navigable MkDocs wiki whose
reference sections are generated from source.

### ✨ Added

**1 · Custom Unity Catalog Volume and Workspace paths.** The Open dialog now takes an explicit path,
with **Browse** to walk directories and **Validate** to check a file before loading it. New
`POST /api/storage/validate` reads through the Files API / Workspace API, parses JSON *or* YAML,
runs the spec validator, and returns a structured report — parse failures and validation failures
are reported distinctly, both as HTTP 200, because "this file has problems" is a normal answer.
Paths are still sanitised server-side and traversal still rejected.

**2 · Open button and the not-applicable toggle.** `Open…` was a ghost button among five others; it
is now a high-contrast primary action labelled **Open spec** with a folder icon.

**3 · Dynamic template directory.** `server/core/templates.py` scans `databricks-app/templates/` on
every request. Drop a `.json` file in and it appears on the next load — no rebuild, no registration.
`index.json` is demoted to an *optional metadata overlay*: it can supply a nicer label or `pinned`,
but a file needs no entry to be listed. Scope comes from the containing directory; each entry
describes itself from its own content, so a catalogue entry cannot drift from the template.

**4 · Documentation wiki.** New `mkdocs.yml` — Material theme, five tabs, nested subsections:

| Tab | Subsections |
|---|---|
| Get started | Step by step (4 guides) · Going further |
| Architecture | Data plane · Cross-cutting concerns · Design guarantees |
| JSON reference | Spec document · Shared blocks · Full attribute dictionary |
| Code reference | Pipeline engine · Platform services · Data handling · Other |
| Help | FAQ · Multi-role FAQs · Known limitations |

New pages: `docs/index.md`, `docs/faq.md` (30 collapsible Q&As), and four onboarding guides
(prerequisites, first pipeline, Spec Builder, deploying). **23 reference pages are generated from
source** by `scripts/build_docs_reference.py` — the JSON reference from the same registry the app
renders, the code reference by parsing `src/` with `ast` (nothing imported, so it builds with no
Spark and no Databricks connection): **58 modules, 28 classes, 165 public functions**.

`docs/archive/` (68 files) and `docs/architecture_review/` (9) are retained on disk but excluded
from the site as superseded content and a dated audit.

**5 · Attribute inspector, for every attribute.** The panel shows purpose, why it matters, a JSON
sample, best practice, known errors with cause and fix, and Databricks documentation links —
inline, without navigating away. Coverage went from 17 hand-written entries to **170 attributes**
via `databricks-app/scripts/build_attribute_knowledge.py`, which derives an entry for every
attribute from the registry and layers hand-written prose on top field-by-field. Curated prose now
lives in `attribute_knowledge.curated.json`; the served file is generated and marked as such.

### 🔧 Fixed

- **Inert fields were editable.** "Show attributes not applicable" greyed fields to `opacity:0.45`
  but left every input live, so typing into a field marked not-applicable **silently discarded the
  value on save** — `buildFlowObject` filters those paths out. They are now `disabled`/`readOnly`,
  carry an `N/A` badge, and state the reason. Verified: 11/11 inert fields disabled, 11/11 give a
  reason.
- **CDC attributes had no inspector coverage.** They are declared on `Builder.jsx::cdcFieldDefs()`
  rather than in `registry.js`, so the first generator missed all 11 — `sequence_by_column`,
  `columns_to_check`, `cdc_operation_column` among them. The dump now bundles `Builder.jsx` through
  esbuild to reach them (160 → 170 attributes).
- **Generator output was mojibake.** `subprocess.run(text=True)` decoded node's stdout with the
  Windows console default, turning every em dash in the registry prose into `â€"`. Encoding pinned
  to UTF-8.
- **`/api/config` grew to 321 KB** once knowledge was inlined — on the SPA's initial load. It is now
  fetched lazily from `/api/attribute-knowledge` on first inspector use; config is back to **92 KB**.
- **A dead documentation link** in `11_hashing_and_determinism.md` pointed outside `docs/` at a
  source file; it now points at the generated code reference. `mkdocs build --strict` passes.

### 📋 Notes

- **`site/` and `node_modules/` are git-ignored.** Build the docs with
  `pip install mkdocs mkdocs-material && mkdocs build`.
- **The app's in-app reference is unchanged.** `/docs/` in the app still serves the single-page
  `docs_site/index.html`, because the inspector deep-links to its anchors and all 21 resolve.
  Repointing it at the multi-page wiki would break those links and add 6.5 MB to the deploy;
  the two are deliberately separate.
- Five templates that existed on disk but were absent from `index.json` (`mv_truncate`,
  `pure_sink`, `snapshot_nopk`, `stream_join`, `union_all`) are now visible — they had been
  invisible to the app.

### ✅ Verification

- `pytest databricks-app/tests` — **60 passed**.
- `databricks bundle validate -t dev_metaflow` — **Validation OK**.
- `mkdocs build --strict` — **45 pages, no warnings**.
- Both generators are deterministic and ship a `--check` mode for CI; re-running produces
  byte-identical output.
- jsdom integration test driving the real React app against a live server: Open-button label,
  template discovery (20) and load-into-flow, path validation report, inert read-only behaviour,
  and lazy knowledge fetch (170 entries) — all pass, **0 console errors**.
- All 44 MkDocs nav entries resolve to real files; the only unreferenced page is the superseded
  `docs/README.md`, explicitly excluded.

---

## Onboarding App v1.5.0 — 2026-08-30

Scope: the **Databricks App** only (`databricks-app/`). No framework, pipeline or control-table
behaviour changed.

Replaces the hand-built v3 SPA with the React/Vite application delivered by Claude Design, adds
spec import, and fixes four integration defects and three serializer defects found while wiring
the two halves together.

### ⚠️ Breaking Changes

**1. The frontend is now React + Vite and must be built.** `web/src/*.ts` (the v3 TypeScript
tree) was removed in favour of the design's `Builder.jsx` / `Shell.jsx` / `registry.js`.
`web/dist/` is now Vite output (`index.html` + hashed `assets/`), not a hand-authored bundle.

*Impact:* `npm install && npm run build` in `databricks-app/web` is required before deploying —
Databricks Apps does not build at deploy time. `node_modules/` (38 MB) is git-ignored.

**2. `source_config.normalize_column_names` removed from the builder.** The legacy boolean is no
longer offered; `source_config.column_normalization.enabled` is the only switch, and
`column_normalization.case` now depends on it alone.

*Impact:* the framework still honours the legacy key at runtime — this removes it from the
authoring UI only. Specs that already set it keep working, and an imported spec containing it is
preserved rather than dropped.

### ✨ Added

| # | Change |
|---|---|
| 1 | **Open an existing spec to edit** — new `Open…` in the header. Upload a `.json`/`.yaml` from disk, or browse the configured Unity Catalog Volume and Workspace roots and open a file in place. Implemented as an exact inverse of `spec()`/`buildFlowObject()`, driven by the same registry definitions, so unrecognised attributes are preserved rather than dropped. |
| 2 | **Templates explain themselves** — each entry in the template browser now derives its summary from the preset body: configuration chips (`source_type`, `strategy`, `format`, …), feature chips (ZIP extraction, PGP decrypt, DQ rules, quarantine, hash columns, liquid clustering, …), and a pre-filled attribute count. Derived, not hand-written, so it cannot drift from the preset. |
| 7 | **Rich inline attribute help** — the `i` drawer no longer just links out. It now shows an applicability banner when a field is inert (with the reason), the description, the hint, the live current value, allowed values as individually-annotated chips (CDC strategies carry their full explanation), the child fields of object/array widgets, and sibling attributes under the same parent. |
| 8 | **Cascading `destination_config` dropdowns** — `file_format` now drives `compression`: `PARQUET` offers SNAPPY/GZIP/NONE, `JSONL`/`JSON` offer GZIP/NONE only (SNAPPY is a Parquet block codec). Changing the format clears a now-invalid compression. Implemented as general `optsOf` / `cascade` hooks on the field definition, reusable by any future dependent pair. |

### 🔧 Fixed

**Integration defects** — the design's frontend and this backend were built against different
contracts. Each would have broken the app in production:

- **`/api/storage/*` did not exist.** The frontend addresses storage under `storage/`; the server
  exposed `workspace/`. Added `server/routers/storage_router.py`, which re-registers the same four
  handlers under the second prefix — one implementation, no forked code path.
- **Config shape mismatch.** The frontend reads `cfg.app.spec_storage.roots` and `cfg.app.actions`;
  the server returned both top-level, so **every save destination would have been empty**.
  `config_router` now emits both, additively.
- **Canonical vs internal spec.** The frontend posts the canonical framework spec; the validator
  expects the internal `{root:{v:…}}` SpecDoc. **Every onboard run failed validation**, and the
  frontend degrades silently to a local walkthrough — so the Databricks job would never have fired
  and no error would have surfaced. Added `as_spec_doc()`, which accepts either shape.
- **`NameError` in the fake Databricks client.** `class RunNowResult: run_id = run_id` makes
  `run_id` class-local, so the right-hand load never reaches the enclosing function. Every
  job-mode action died with `UPSTREAM_ERROR` under `METAFLOW_FAKE_DBX`.

**Serializer defects** — these produced specs the framework would reject:

- **List children of repeatable groups were saved as comma-strings, not arrays.** `itemObj` split
  only two hardcoded names, so `destination_config.event_log_tables` was emitted as
  `"a_tbl,b_tbl"` instead of `["a_tbl","b_tbl"]`. Splitting is now driven by the registry's own
  `k === "list"` declaration.
- **Observability list children had the same bug** on a separate code path in `spec()`; fixed the
  same way.
- **`decrypted_columns` was lost on import.** `buildFlowObject` folds the flat list back into
  `out.source_inputs`, then writes `source_inputs` from its own key — so inserting
  `decrypted_columns` first meant the later write overwrote the merge. Ordering corrected.

**Docs** — `docsBase()` appended `00_master_reference_index/`, which does not exist, so every
`i`-drawer and section link 404'd. Three registry anchors were also stale
(`#1-top-level-spec-attributes`, `#2-ingestion-flow-fields`,
`#3-source-config--common-fields-all-source-types`). All **17 section anchors now resolve**.

**Copy** — `delete_source_after_extract.action` no longer describes the legacy boolean
(item 3); `pre_extraction_decryption.secret_passphrase.*` now appears only once
`pre_extraction_decryption.type` is chosen, instead of whenever ZIP handling was enabled (item 4).

### 📋 Note on item 6

`capture_technical_metadata`, `partition_columns`, `liquid_clustering_columns` and
`auto_ttl.*` were **not missing**. Evaluating the registry against default values confirms
`capture_technical_metadata` renders in the **Reader** phase and the rest in **Storage** —
steps 3 and 5 of the 9-step wizard. `partition_columns` is correctly withheld until
`partition_columns mode` is set to `named`. The **Show attributes not applicable** toggle in the
left rail reveals every gated field with the reason it is inert; the enriched info drawer now
states that reason explicitly.

### ✅ Verification

- `pytest databricks-app/tests` — **60 passed**.
- `databricks bundle validate -t dev_metaflow` — **Validation OK**.
- **Round-trip proof:** a harness driving the real `Builder` component (bundled with esbuild,
  no DOM) loads a canonical spec and re-serialises it. Ingestion (kv objects, lists, repeats,
  nested `auto_ttl`, booleans), transformation (`source_inputs` + `decrypted_columns`),
  reconciliation and observability all return **0 diffs**.
- Cascade verified across all four `file_format` values, including that switching
  `PARQUET`+`SNAPPY` → `JSONL` clears the compression and → `PARQUET` does not.
- `/api/storage/{list,read,write,access}` verified byte-identical to their `/api/workspace/*`
  counterparts against a live uvicorn process.

---

## Docs — Known Limitations KB + stability test plans — 2026-08-30

Documentation and test-planning only. **No framework, pipeline, app or control-table behaviour
changed.**

### ✨ Added

| File | What it is |
|---|---|
| [`docs/13_known_limitations_and_gotchas.md`](docs/13_known_limitations_and_gotchas.md) | New KB: ~50 verified traps the onboarding validator cannot catch, in one hyperlinked summary table graded 🔴 Silent / 🟠 Late failure / 🟡 Inert / 🔵 Operational, then a detail section per trap. Written for someone filling in an onboarding JSON. |
| [`metaflow_testing/STABILITY_TEST_PLAN.md`](metaflow_testing/STABILITY_TEST_PLAN.md) | Consistency/stability plan for a new workspace: 4–5 runs per test case under a fixed N1–N5 protocol, 7 cross-run invariants, 4 schemas + 4 volumes (down from 70/82), 6 deep-dive suites, 7 predictions on record. |
| [`metaflow_testing/DATA_VARIATION_TEST_PLAN.md`](metaflow_testing/DATA_VARIATION_TEST_PLAN.md) | Earlier, narrower data-variation plan against `dev_metaflow`; superseded by the above but retained for its per-wave detail. |

`docs/README.md` index updated with the new module 13 row.

### 🔍 Findings established while writing these (code verified, not yet re-run live)

* **`source_zip_handling.target_volume_path` is never cross-checked against `source_config.path`.**
  It appears exactly once in `spec_validator.py`, as a `check_string`. A mismatch extracts the
  archive successfully, then reads a directory the members were never written to — **zero rows,
  no error, on a SUCCESS-reporting update**. Documented as [S1](docs/13_known_limitations_and_gotchas.md#s1).
* **`schema_config` does not project.** `apply_schema_config` appends every undeclared column as
  passthrough, so a 4-column `schema_config` over a 50-column source lands all 50. No
  `select_columns`/`include_columns`/projection field exists anywhere in `source_config`; the
  supported narrow-table route is a transformation flow. [C1](docs/13_known_limitations_and_gotchas.md#c1).
* **Zero live coverage** for `column_normalization` / `normalize_column_names` /
  `schema_config_path` (no spec in the 44-spec corpus enables any of them), and zero coverage for
  the AES ZIP passphrase path (`pre_extraction_decryption.secret_passphrase`).
* **`pipelines.maxFlowRetryAttempts` is unset everywhere**, so it defaults to 5 for triggered
  pipelines — a transiently-failing flow is retried and the update still reports SUCCESS.
  [O3](docs/13_known_limitations_and_gotchas.md#o3).
* **Unresolved discrepancy:** `ingestion/column_normalization.py`'s docstring says normalization
  runs *before* `apply_schema_config`; `notebooks/03_engine/03_lakeflow_declarative_pipeline.py`
  runs `apply_schema_config` first. Recorded as an open item in doc 13; the notebook is
  authoritative until a live run settles it.

---

## Onboarding App v1.4.0 — 2026-08-30

Scope: the **Databricks App** only (`databricks-app/`). No framework, pipeline or control-table
behaviour changed in this release.

Imports the v4 "MetaFlow Spec Builder" design from Claude Design, implements its design system
as a stylesheet, and consolidates the two parallel app directories down to one.

### ⚠️ Breaking Changes

**1. App source directory renamed.** `metaflow-onboarding-app/` was removed; the app now lives
at `databricks-app/`. `resources/metaflow_onboarding_app.yml` `source_code_path` was repointed
to `../databricks-app`, and the `.gitignore` un-ignore rules for the built frontend were
updated to match. The deployed app `name` (`metaflow-onboarding`) is **unchanged**, so this is
a source-tree move only — it does not orphan or recreate the deployed app.

*Impact:* any local script, editor bookmark or CI path referencing `metaflow-onboarding-app/`
must be updated. `databricks bundle validate` passes against the new path.

### ✨ Added

| Area | Change |
|---|---|
| Design | `databricks-app/web/preview.html` — v4 layout reference imported from Claude Design. Static; no API and no persistence. Self-contained interaction script (theme toggle, preview-pane collapse, modals, info drawer, phase/tab/flow single-select). |
| Design system | `databricks-app/web/dist/styles.css` — new stylesheet implementing the full v4 vocabulary: **154 classes**, ~800 lines. Three-pane app shell with independent scroll per pane, header segmented control and split button, flow nav with completion states, phase rail with status dots, two-column field grid, strategy tabs, key-value editor, repeatable groups, run modal with animated stage rings, attribute index, and info drawer. |
| Theming | Dual theme on the existing `data-mfl` attribute, reusing the established token vocabulary (`--acfill`, `--panel3`, `--bd4`, `--ac2`, …). Every colour token has a light counterpart; only geometry and typography tokens are theme-neutral. `prefers-reduced-motion` respected. |
| History | `dev_logs/` (10 files, app scaffolding through UI redesign) carried into `databricks-app/`. |

### 🔧 Fixed

- `databricks-app/web/dist/` was missing `index.html` and `app.js`, so the FastAPI SPA mount
  (`server/app.py::get_static_dist_dir`) resolved to no candidate and the app served nothing at
  `/`. The built SPA was restored into the new location.

### 📋 Status — not yet complete

The v4 work in this release is **design-only**. The application served at `/` is still the v3
SPA (`web/dist/index.html`, a self-contained 121 KB build inlining its own CSS and JS). The v4
stylesheet and the v3 SPA share only **6 of 155** class names, so v4 is not reachable from the
running app yet.

Outstanding before v4 can ship:

1. **No frontend build tooling.** There is no `package.json`, `tsconfig.json` or bundler config,
   so `web/src/*.ts` (`main.ts`, `components/`, `engine/`, `state/`) cannot be compiled. The
   committed `web/dist/` artefacts are the only frontend that runs.
2. **v4 markup not implemented in the app.** `preview.html` is a static reference; its structure
   has to be reproduced by the TypeScript components before the new stylesheet takes effect.
3. **Visual fidelity unverified.** The stylesheet was validated structurally — 155/155 classes
   resolved, braces balanced, all 42 referenced tokens defined, both relative asset paths
   resolve — but has not been rendered in a browser and compared against the design.

### ✅ Verification

- `pytest databricks-app/tests` — **59 passed**, before and after removal of the old directory.
- `databricks bundle validate -t dev_metaflow` — **Validation OK**.
- No stale `metaflow-onboarding-app` references remain in `resources/`, `databricks.yml` or
  `.gitignore` (remaining hits are confined to generated `.databricks/` deploy state, which
  refreshes on next deploy).

---

## v1.3.00 — 2026-08-29

Thirteen framework enhancements across ingestion, CDC, storage, reconciliation and
observability, plus a new generic bulk config-onboarding capability. Implemented via
multi-agent orchestration against a frozen design contract, with every stream independently
adversarially verified; five confirmed defects were found by that verification and fixed
before release.

Full detail: [`enhancement_logs/v1.3.00_enhancement_log.md`](enhancement_logs/v1.3.00_enhancement_log.md).

### ⚠️ Breaking Changes

**1. Deterministic hashing standard (E08).** Every column participating in
`__framework_hash_key`, `__framework_hash_value` and `__framework_surrogate_key` is now
normalized as `trim(lower(cast(col as string)))` before hashing, joined with `||`, and
digested with `sha2(..., 256)`. Previously the construction was
`sha2(concat_ws('||', coalesce(cast(col as string), ' NULL ')), 256)` — no trim, no lower —
and the surrogate-key generator maintained its own separate copy of it.

*Impact:* hash values computed after this release **will not match** those already
materialized in existing CDC target tables. Reconciliation batch fingerprints also change, so
the first post-upgrade reconciliation run per target will not recognize previously-`SUCCESS`
batches and will re-append once (expected, not a bug).

*Migration:* full-refresh CDC targets whose `__framework_hash_*` columns you depend on, or
accept a one-time re-comparison. See
[`docs/11_hashing_and_determinism.md`](docs/11_hashing_and_determinism.md) for the exact
expression, a reproducible Spark SQL snippet, and the migration checklist.

**2. Reconciliation is Delta-tables-only (E12d).** A `reconciliation_flows[].source_config.type`
other than `"table"` is now rejected at onboarding. Previously `type: "file"` was accepted and
validated for `path`/`format`. Read file/sink output into a Delta table first, then reconcile
against that table.

**3. `landing_retention_policy` no longer errors on a missing `archive_path` (E01).** A
`clean_source: "archive"` policy with a missing or empty `archive_path` now **degrades to
`off`** with a runtime warning instead of raising a validation error. This is more permissive,
not less — but any tooling asserting on the old `archive_path: is required` error string must
be updated.

### ✨ Features Added

| ID | Feature |
|---|---|
| E01 | `landing_retention_policy` — `clean_source` `archive`/`delete`/`off`, default `retention_days` 7, `retention_days: 0` now valid, `delete` never requires `archive_path`. Auto Loader only; never applied to raw ZIP pre-extraction. |
| E02 | `source_zip_handling.delete_source_after_extract` accepts a nested object — `{"action":"delete_now"}` or `{"action":"delete_after_x_days","days":N}` — alongside the legacy boolean. `delete_after_x_days` runs an age-based sweep of the landing directory. |
| E03 | An **explicitly-present but empty** `explode_columns: []` auto-flattens every nested struct and explodes every array. An **absent** key stays schema-preserving pass-through. Parquet parity for JSON-string columns. |
| E04 | `source_config.remove_dups` (bool, default `false`) — full-row streaming deduplication, excluding `__framework_*` columns and `_rescued_data`. |
| E05 | `source_config.column_normalization` object (`enabled`, `case`: `lower`/`preserve`/`upper`) layering over the legacy `normalize_column_names` boolean. |
| E06 | `target_config.partition_columns: []` explicitly configures an **unpartitioned** table — valid, and distinct from omitting the field. |
| E07 | `target_config.liquid_clustering_columns` capped at **3 columns**, enforced at onboarding (`maxItems: 3`) and again at runtime. |
| E08 | One canonical deterministic hashing implementation shared by ingestion, transformation and reconciliation (see Breaking Changes). |
| E09 | ~~`target_config.empty_target_if_source_empty` (bool, default `false`) guards `TRUNCATE_AND_LOAD`.~~ **WITHDRAWN 2026-08-29 — not enforced.** Preserving the target requires the target to read itself, which Lakeflow rejects at graph construction (`Graph is not topologically sorted. There is a cycle between <target> and <target>`), and the guard's eager emptiness test cannot tell "source is empty" from "source not yet materialized" during graph construction. Every `TRUNCATE_AND_LOAD` pipeline failed outright (TC-CDC-002). The option is accepted by the schema but has no runtime effect; enforcing it needs a post-update check outside the pipeline graph. |
| E10 | `FULL_SNAPSHOT_CDC_NO_PK` no longer requires an explicitly configured surrogate key; the framework generates and tracks `__framework_surrogate_key` internally. New optional `surrogate_key_columns` / `surrogate_key_exclude_columns` pin its basis. |
| E11 | Hierarchical Spark configuration (`engine/spark_config.py`) — pipeline/runtime values strictly override framework defaults (e.g. `spark.sql.shuffle.partitions` `200` → `auto`). |
| E12 | Reconciliation: `triggered` (bounded, `task_run_id`-filtered) vs `continuous` (streaming) modes; `recon_run_log_capture` / `recon_mismatch_log` runtime overrides; two-tier verification with a cheap Phase 1 fingerprint escalating to Phase 2 only on mismatch. |
| E13 | Observability: triggered mode (task-bounded `event_log` window) and continuous mode (array of fully-qualified `catalog.schema.event_log_table` names streamed together to Volumes or OTel). |
| — | **Generic bulk config-onboarding job** — `framework_config_onboarding_job` onboards every spec in a directory in one run, fail-soft with a full per-spec report. Tagged `purpose: testing`. |

### 🐛 Bug Fixes

Five defects were found by the adversarial verification pass and fixed before release. All
five were independently re-verified with executed proof probes:

1. **[Critical]** `resolve_truncate_and_load_source` (E09) had **zero callers** — the entire
   empty-source truncate guard was dead code, so a `TRUNCATE_AND_LOAD` flow with a zero-row
   source still blanked its production target. It was wired into
   `dq/quarantine.py::_clean_upstream` — and then **withdrawn again on 2026-08-29**, because live
   execution showed the approach is inexpressible in a Lakeflow graph: the wired-in guard made the
   target read itself and every `TRUNCATE_AND_LOAD` pipeline failed graph construction with a
   self-cycle. See Known Issues.
2. **[Critical]** An explicit `generate_surrogate_key: false` on `FULL_SNAPSHOT_CDC_NO_PK`
   raised `CdcStrategyError` — the opposite of E10's goal — and `surrogate_key_columns` /
   `surrogate_key_exclude_columns` were read nowhere. Now forced on with a warning, both
   column lists threaded through.
3. **[Major]** `delete_after_x_days` (E02) excluded every archive matched in the current run
   from its own sweep — a set identical, by construction, to its candidate set — making it
   provably unable to delete anything, while the retained archive was silently re-extracted on
   every update. Now excludes only archives whose extraction *failed* this run.
4. **[Major]** A `column_normalization` object present but omitting `enabled` silently
   disabled normalization even with `normalize_column_names: true` set — the most natural
   adoption path for existing users. Absence of `enabled` now defers to the legacy boolean.
5. **[Major]** `resolve_surrogate_key_columns` treated a present-but-empty
   `include_columns: []` as absent (truthiness check), silently widening a surrogate key to
   every column. Now tests for `None`.

Additionally, one **E13 delivery gap** was caught during documentation cross-checking and
fixed: `onboarding/metadata_upsert.py::upsert_observability_config` never wrote the `mode`
column, so a spec-declared `"mode": "continuous"` destination landed with `mode` NULL and was
resolved to `triggered` by `config_loader.py` — making continuous-mode destinations
unreachable through the documented onboarding path.

#### Live pipeline-execution fixes (2026-08-29)

Five named pipelines were failing on `dev_metaflow`. Each was diagnosed from its own Lakeflow
event stream and fixed; **all five were real defects**, and four were introduced or left latent by
this release. Full detail in `metaflow_testing/TESTING_STATUS.md` §0a.

6. **[Critical]** `cloudFiles.fileNamePattern` **is not a valid Auto Loader option for any
   format.** Auto Loader validates `cloudFiles.`-prefixed keys against a closed whitelist without
   consulting `cloudFiles.format`, so the earlier "use `pathGlobFilter` only for `binaryFile`" fix
   was half a fix — every other format still emitted the invalid key and would have failed the
   moment a spec set `file_pattern`. `TC-ING-004` is the only spec that does, which is why nothing
   else surfaced it. **Fixed**: `file_pattern` maps to the generic, un-prefixed `pathGlobFilter`
   for every format. (`tests/unit/test_autoloader_file_pattern.py`)
7. **[Critical]** **A snapshot lambda may not reference any pipeline dataset.**
   `FULL_SNAPSHOT_CDC[_NO_PK]` passed a lambda to `apply_changes_from_snapshot` that called
   `dlt.read()`. Lakeflow rejects that — as `TABLE_OR_VIEW_NOT_FOUND` for a view and
   `REFERENCE_DLT_DATASET_OUTSIDE_QUERY_DEFINITION` once materialized. **Fixed**: the strategy
   registers a real `@dlt.table` snapshot-input dataset (delete-value filter and NO_PK guard
   inside it) and passes `apply_changes_from_snapshot` that dataset's **name**; `is_streaming` is
   threaded from `flow_registration.py` so the upstream is read with the matching API.
   (`tests/unit/test_snapshot_input_dataset.py`)
8. **[Major]** **Concurrent `setup_control_tables` runs failed each other.** Unity Catalog's
   `CREATE OR REPLACE FUNCTION` is idempotent in intent but not atomic; the loser of a race gets
   `[ROUTINE_ALREADY_EXISTS]`, failing the job and skipping every downstream task. **Fixed**: a
   narrow `is_already_exists_race()` predicate shared by `schema_provisioner.py` and
   `01_setup_control_tables.py`. Permission, missing-schema, quota and syntax errors still fail
   loudly. (`tests/unit/test_already_exists_race.py`)
9. **[Major, test fixture]** The zerobus seeders MERGEd with `whenMatchedUpdate()`, rewriting
   already-present rows on every re-seed. A Delta **streaming source must be append-only**, so the
   second seed permanently broke `zerobus_bronze` with `DELTA_SOURCE_TABLE_IGNORE_CHANGES`.
   **Fixed**: both seeders are insert-only — equally idempotent, and a truer model of an event bus.

### 🧩 Templates & Attribute Reference (v1.3.0 completeness pass)

All onboarding templates were audited against the shipped code and brought up to date — 12
v1.3.0 attributes were present in `onboarding_spec.schema.json` but **missing from every
template**:

- `onboarding_templates/pipeline_onboarding_template.json` / `.yaml` — now demonstrate
  `spark_config`, `json_string_columns`, `remove_dups` + `dedup_watermark`,
  `column_normalization`, `landing_retention_policy`, `partition_columns: []`,
  `liquid_clustering_columns`, `empty_target_if_source_empty`, `surrogate_key_columns` /
  `surrogate_key_exclude_columns`, `recon_mode`, `two_tier_verification`, `logging_config`,
  observability `mode` + `destination_config.event_log_tables`. The YAML is regenerated from
  the JSON and verified structurally identical.
- `onboarding_templates/onboarding_spec_full_reference.json` / `.md` — same additions, plus a
  net-new `observability[]` array (previously absent from this reference entirely) covering
  both `triggered` and `continuous` destinations, and an extended field-to-location index.
- `agent_skills/reference/onboarding_spec_full_reference.json` — re-synced (hash-verified).

**Every template now validates against the real `validate_spec` with zero errors.** That check
surfaced three latent defects that had been shipping in the templates:
1. `pipeline_onboarding_template.json` referenced `${run_date}` with no matching
   `pipeline_parameters` entry — an undefined-parameter error. Fixed.
2. `onboarding_spec_full_reference.json` used reconciliation targets of `type: "file"` and
   `type: "sink"`, which E12d's Delta-only restriction now rejects. Converted to the
   documented read-back-table migration path.
3. The same file used `file_format: "jsonl"` and `auth.type: "bearer"` (wrong casing) and an
   `auth` block missing its required `credentials` object.

A schema-vs-code attribute audit was also run across every property in
`onboarding_spec.schema.json`: one documented dead field (`sink_config.write_mode`, already
annotated as such) and six optional passthrough fields that are consumed by
`metadata_upsert.py`/`readers.py` but not explicitly type-checked by the validator
(`source_system`, `source_database`, `source_table_name`, `source_description`,
`starting_version`, `max_bytes_per_trigger`). No orphaned or undocumented attributes.

### 📋 Configuration Schema Modifications

`onboarding_templates/onboarding_spec.schema.json`:
- `source_config`: added `remove_dups`, `column_normalization` (object), `json_string_columns`;
  `explode_columns` now permits an explicitly-empty array; `landing_retention_policy.retention_days`
  minimum lowered to `0`; `archive_path` no longer required for `clean_source: "delete"`;
  `delete_source_after_extract` became a `oneOf` (legacy boolean | nested action object).
- `target_config`: added `empty_target_if_source_empty`, `surrogate_key_columns`,
  `surrogate_key_exclude_columns`; `liquid_clustering_columns` gained `maxItems: 3`;
  `partition_columns: []` documented as explicitly-unpartitioned.
- `reconciliation_flows`: `source_config.type` restricted to `"table"`; `recon_mode` and
  `two_tier_verification` surfaced.
- `observability[]`: `mode` (`triggered`/`continuous`) and
  `destination_config.event_log_tables` surfaced.

`control_plane/ddl_definitions.py`: `observability_config.mode`,
`reconciliation_flow_spec.recon_mode` / `.two_tier_verification` columns; several column
comments updated. All statements remain `CREATE TABLE IF NOT EXISTS` — **existing
deployments do not auto-gain new columns.**

### 📚 Documentation

Updated: `docs/00`–`docs/03`, `docs/07`, `docs/08`, `docs/README.md`.
New: [`docs/11_hashing_and_determinism.md`](docs/11_hashing_and_determinism.md) (canonical
hashing standard + reproducible SQL + migration note) and
[`docs/12_module_permutation_matrix.md`](docs/12_module_permutation_matrix.md) (the
cross-module topology matrix across Ingestion × Transformation × Reconciliation ×
Observability). `docs/archive/` deliberately untouched.

A documentation cross-check pass against the shipped source found and corrected **23 factual
inaccuracies**, most of them pre-dating this release (fictitious field names such as
`compute_hash_key`, `table_name`/`starting_offset` for Zerobus, `codec`/`top_level_type` for
ASN.1, `name`/`type` in `schema_config`, non-existent `__framework_quarantine_*` columns,
non-existent exception classes, and broken cross-reference anchors).

### ✅ Test Suite Coverage & Multi-Agent Execution Metrics

**Offline / local:**
- `pytest tests/unit --confcutdir=tests/unit`: **449 passed**, 8 failed, 109 errors.
  The 8 failures and 109 errors are pre-existing and environmental (deleted legacy
  `test_specs/*.json` fixtures; the `spark` fixture being cut off by `--confcutdir`) — both
  confirmed identical against a pre-change backup.
- `ruff check --select F,E9`: **0 new findings** (3 pre-existing unused imports untouched).
- `bundle validate` + `bundle deploy`: OK on both targets.

**Live (bulk onboarding):** the new `framework_config_onboarding_job` onboarded
**43/43 specs successfully** — 55 ingestion, 8 transformation, 6 reconciliation flows and 3
observability destinations — on both workspaces. This is a genuine end-to-end validation of
the new JSON schema, validator and control-table DDL against the entire real spec corpus.

**Live (test jobs):** the 44-job `TC-*` backlog was executed live for the first time. Detailed
per-test-case results, run IDs and root-cause analysis are in
[`metaflow_testing/TESTING_STATUS.md`](metaflow_testing/TESTING_STATUS.md) §0.

**Multi-agent metrics:** 1 contract-freezing agent → 9 file-disjoint implementation streams +
9 adversarial verifiers → 4 remediation streams + 4 re-verifiers → 1 doc planner + 8 doc
writers + 8 doc cross-checkers + 7 doc-fix agents. ~60 agent invocations, ~4.4M subagent
tokens. The verification layers earned their keep: they caught 5 real code defects (2 critical)
and 23 documentation inaccuracies that the implementation and writing passes had each
self-reported as complete.

### 🚧 Known Issues & Environment Nuances

**1. `TC-ING-004` (ASN.1) cannot start — pre-existing, not fixed in this release.**
```
[CF_UNKNOWN_OPTION_KEYS_ERROR] Found unknown option keys: cloudFiles.filenamepattern
```
**FIXED on 2026-08-29 — this is no longer a known issue.** `cloudFiles.fileNamePattern` is
not a valid Auto Loader option for **any** format: the `cloudFiles.` key whitelist is checked
without consulting `cloudFiles.format`. `_apply_common_autoloader_options` now emits the generic,
un-prefixed `pathGlobFilter` for every format, and `TC-ING-004` passes live.

> **Do NOT set `cloudFiles.validateOptions=false`.** It was listed here as an alternative
> workaround before the real fix landed, and it does not fix anything — it only silences the
> validator so an invalid key is ignored. If you still hit
> `CF_UNKNOWN_OPTION_KEYS_ERROR: cloudFiles.filenamepattern`, your pipeline is pinned to a
> **pre-fix wheel**; repoint it at `0.0.1788023654366` or later (the UC Volume retains every
> published wheel, so old pins keep working — and keep failing — indefinitely).

**2. Free-tier workspace resource quotas gate large test runs.** Running the full 44-job
corpus concurrently exhausts two hard limits:
- `QUOTA_EXCEEDED.UC_RESOURCE_QUOTA_EXCEEDED` — Unity Catalog allows ~50 schemas per catalog
  and ~50 volumes per metastore. The corpus collectively wants more.
- `RESOURCE_EXHAUSTED: You've hit the limit for severless compute for free usage` — hit when
  several Lakeflow pipelines start at once.

Both fail in the `setup_control_tables` / `seed_*_data` / `onboard_*` tasks, i.e. **before**
any framework code runs, so they say nothing about correctness. Mitigation: run in waves at
low concurrency (≤3), and keep catalog/volume headroom.

**3. `mapInPandas` / Python UDFs fail under Databricks Connect, not on job compute.**
Corrects a previously-documented workspace-wide constraint. Measured both ways:

| Probe | Databricks Connect (local) | Serverless job compute |
|---|---|---|
| plain Spark | OK | OK |
| Python UDF | **FAIL** `ISOLATION_STARTUP_FAILURE.SANDBOX_STARTUP` | **OK** |
| `mapInPandas` | **FAIL** (same) | **OK** |

ASN.1 and ZIP/PGP paths are **not** platform-blocked when run as jobs. What *is* affected is
local `pytest` against the `spark` fixture in `tests/conftest.py`.

**4. Control-table columns are not auto-migrated.** New columns (`observability_config.mode`,
`reconciliation_flow_spec.recon_mode` / `.two_tier_verification`) only appear in freshly
provisioned catalogs, because `01_setup` uses `CREATE TABLE IF NOT EXISTS`. Existing
deployments need a manual `ALTER TABLE ... ADD COLUMNS`. Both are read defensively (NULL
resolves to the pre-v1.3.0 default), so an un-migrated deployment keeps working.

**5. `agent_skills/*.json` tool specifications were not updated** with the new v1.3.0 config
surface. An AI agent driving onboarding through those specs will not know the new fields
exist. Recommended as the next follow-up.

**6. `crypto/column_crypto.py`'s plaintext-key-in-plan workaround remains open** from the
prior architecture-review remediation — unrelated to this release's scope.
