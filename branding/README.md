# `branding/` — the one place you change the brand

This directory is the central branding layer. `branding.json` is the single source of truth for
every user-visible brand string and logo asset in the repo: nothing else may *decide* the brand.

Be clear about what that does and does not buy you. Editing `branding.json` and running one command
updates the **generated** surfaces (the app's served title, its env-var names, the frontend wordmark,
the logo assets) — but roughly **45 files carry the brand as literal text** in static YAML, Markdown,
JSON and HTML that cannot read a config file. Those are listed below and must be edited deliberately.

What keeps that honest is `tests/unit/test_branding.py`: every surface in the list is asserted against
`branding.json`, so a **disagreement fails the build** instead of silently shipping half a rename.
A rebrand is therefore: edit one config, run one command, work the hand-edit list, and let the test
tell you what you missed.

```bash
python scripts/apply_branding.py        # regenerate derived artifacts
cd databricks-app/web && npm run build  # web/dist/ is git-tracked and IS what the server serves
```

| File | What it is |
|---|---|
| `branding.json` | The config. **This is the file you edit.** |
| `branding.py` | Stdlib-only loader for repo-side Python. `load_branding()`, `get("app.title")`, `env_var("SPEC_CATALOG") -> "METAFLOW_SPEC_CATALOG"`. |
| `README.md` | This file. |

---

## The two-mechanism design

There are only two ways a brand string reaches a surface, and it matters which one you are looking at:

1. **Generated files** — written by `scripts/apply_branding.py` from `branding.json`. Never hand-edit
   them; your edit is silently overwritten on the next run. Each carries a `GENERATED FILE` banner.
2. **Hand-maintained files that read the config at runtime** — they `import` `branding.py` (repo side)
   or `branding_generated.py` / `branding.js` (app side).

`apply_branding.py` deliberately does **not** rewrite arbitrary source files with a regex. This repo has
been burned twice by exactly that: a blanket `sed` mangled Python identifiers during a column rename,
and a non-greedy DOTALL `re.sub` swallowed ~650 lines of `Builder.jsx`. A generator that only writes
whole generated files cannot corrupt hand-written code.

---

## What `scripts/apply_branding.py` regenerates

| Generated file | Purpose |
|---|---|
| `databricks-app/web/src/branding.js` | Frontend accessor. Imported by `Shell.jsx` etc. |
| `databricks-app/server/branding_generated.py` | App-side config, embedded (see *Runtime packaging* below). |
| `databricks-app/web/public/logo-light.png` | Cropped, transparent-background mark for the **light** theme. |
| `databricks-app/web/public/logo-dark.png` | Cropped, transparent-background mark for the **dark** theme. |
| `docs/assets/logo.png` | MkDocs theme logo. |

`--check` exits non-zero if any of these is stale, for CI:

```bash
python scripts/apply_branding.py --check
```

The script is idempotent — running it twice in a row writes nothing the second time.

---

## Runtime packaging: why the app gets its own generated copy

**The Databricks App cannot import `branding/branding.py`.** The app resource sets

```yaml
source_code_path: "../../databricks-app"      # resources/flowx_app/flowx_onboarding_app.yml:21
```

Databricks Apps uploads exactly that directory and nothing else, so a top-level `branding/` sibling is
never shipped. Nor does it arrive another way: the only `sys.path` insert in the app
(`server/app.py`) points at `databricks-app/` itself and goes *down*, never up to the repo root; and
`databricks-app/requirements.txt` has no dependency on the framework wheel. A JSON file would have
the same problem — it must live under `databricks-app/` to exist at runtime.

So `apply_branding.py` emits `databricks-app/server/branding_generated.py`, which embeds the values
directly and imports nothing outside the stdlib. Inside the app:

```python
from server.branding_generated import APP_TITLE, env_var
```

Repo-side Python that runs from a checkout uses `branding/branding.py` instead — today that is
`scripts/bootstrap_workspace.py`, whose `--help` description and operator banner are the two brand
strings it prints. The two accessors never diverge because both are produced from the same
`branding.json`, and `--check` fails CI if the generated copy is stale.

> Most repo-side files do **not** read it, and cannot: `notebooks/`, `src/flowx/` docstrings and every
> Markdown/YAML/JSON surface carry the brand as literal text. They are listed under *Files a customer
> must still edit BY HAND* below. Do not assume a file is wired just because an accessor exists.

### Why the frontend gets a generated `.js` rather than importing the JSON

`databricks-app/web/vite.config.js` declares **no `root`** and **no `server.fs.allow`**. Vite therefore
defaults `root` to `databricks-app/web/` and the fs allowlist to that same directory, so a module
outside `web/` cannot be imported by the frontend build — the dev server blocks it. Generating
`web/src/branding.js` *into* the Vite root is the approach that actually builds, and it was verified by
running a real `vite build` against it: the bundler resolved, transformed and tree-shook the module
into the output. An import of `../../../branding/branding.json` was rejected as the design for this
reason, not merely suspected to be risky.

---

## Logo assets — and a correction to the earlier recon

Sources live in `logo/` at the repo root. The filenames contain **a space and an uppercase `.PNG`**
extension; `branding.json` records them verbatim and the generator handles that.

The mapping is the **opposite** of what the pre-work recon assumed. Verified by decoding the pixels:

| Source | Background | Wordmark text | Correct theme |
|---|---|---|---|
| `logo/Logo White.PNG` | white `(255,255,255)` | dark grey `(59,56,56)` | **light** |
| `logo/Logo Black.PNG` | black `(0,0,0)` | white `(255,255,255)` | **dark** |

"Logo Black" is named for its *background*, not its ink. Both carry the identical multicolor NRM glyph
(purple, orange-red, pink, teal, blue).

Neither is a drop-in replacement: both are 1280x720 with heavy 16:9 padding and an **opaque** baked-in
background, so at `.brand-logo { height: 22px }` the untouched asset renders as a tiny mark inside a
solid box. `apply_branding.py` therefore crops each to its content bounding box and converts the flat
background to alpha (feathering the anti-aliased rim), producing 711x333 and 674x303 transparent PNGs.
This runs on the **stdlib only** (`zlib` + `struct`) because Pillow is not a dependency of this repo.

> **The `filter: invert(1) hue-rotate(180deg)` rule in `theme.css` must be REMOVED.** It exists to make
> the old dark-on-transparent hoonartek mark visible on the dark theme. The NRM mark is multicolor, and
> inverting it destroys the brand colors. Light/dark is now handled by picking between the two assets
> (`branding.logoLight` / `branding.logoDark`), not by a CSS filter. The paired
> `:root[data-mfl="light"] .brand-logo { filter: none; }` override becomes dead and should go with it.
> Do **not** rename the `data-mfl` attribute — it is matched by the theme toggle in `Builder.jsx`.

The favicon is intentionally untouched: `databricks-app/web/public/favicon.svg` is a generic
Databricks-red glyph carrying no FlowX or hoonartek mark, so the rebrand as scoped does not require
changing it. An NRM-branded favicon is a **new** decision.

---

## Files a customer must still edit BY HAND

`apply_branding.py` regenerates generated files only. These carry brand strings in hand-maintained
source and must be edited deliberately, each with its own gate. This list is the honest scope — nothing
here is silently handled for you.

**Frontend** (then `npm run build`; `web/dist/` is git-tracked and is what `server/app.py` serves)
- `databricks-app/web/index.html` — `<title>` (hardcoded). Static HTML parses before any module
  loads, so it cannot read `branding.js`. It is **not** generated either: it is hand-maintained
  source carrying the favicon link, font preconnects, the root div and the module script tag, so
  generating it would either put hand-written frontend markup under the generator's ownership or
  force the in-place regex rewrite this script exists to avoid.
- `databricks-app/web/preview.html` — `<title>` (hardcoded, same reason; no build step, served as-is)
- `databricks-app/web/src/Shell.jsx` — the `<img>` `alt`/`src`, and the line-4 comment. The header
  **wordmark and subtitle now read `branding.js`** (`branding.frameworkName` /
  `branding.builderSubtitle`) and need no edit when the brand changes — only `npm run build`.
- `databricks-app/web/src/theme.css` — the `.brand-logo` comment **and removing the `filter:` rule**
- `databricks-app/web/src/registry.js` — header comment
- `databricks-app/web/package.json` — `name`; then regenerate the lock with `npm install` (do **not**
  hand-edit `package-lock.json`, which mirrors it in two places)

**App backend**
- `databricks-app/config/index.json` — `app.title` (the title actually served)
- `databricks-app/config/docs.json`, `server/settings.py` — the placeholder docs URL
- `databricks-app/config/attribute_faqs.json` — FAQ prose rendered in the attribute help panel.
  Static data with no runtime accessor. Today exactly **one** answer names the framework; anchor on
  the whole sentence, never a bare `FlowX` regex — the file is ~300 KB. Do **not** rename any spec
  attribute or JSON key while you are in there.
- `databricks-app/app.yaml` — the `METAFLOW_APP_CONFIG` / `METAFLOW_LOG_LEVEL` names. Static YAML,
  read before any Python runs. The bundle app resource's `config:` block supersedes this file at
  deploy time, so it governs local `uvicorn` runs only — which is exactly why a mismatch here is
  easy to miss.
- `server/app.py`, `server/logging_setup.py`, `server/core/*.py`, `server/errors.py` — docstrings and
  user-visible strings
- The `FLOWX_*` → `METAFLOW_*` env vars: every **reader** (`server/settings.py`,
  `logging_setup.py`, `deps.py`, `clients/dbx.py`, `clients/jobs.py`, `routers/config_router.py`) and
  every **writer** (`databricks-app/app.yaml`, `resources/flowx_app/flowx_onboarding_app.yml`) must move
  in the same commit, plus the test setters. Use `env_var()` so the prefix comes from config.

> **The env-var trap.** The bundle also writes the unbranded aliases `ONBOARDING_JOB_ID` and
> `DATABRICKS_ONBOARDING_JOB_ID`, and settings falls back to `DATABRICKS_HOST`. If you rename a writer
> but miss a reader, **the app keeps working via the alias** — the rename looks successful and the defect
> stays hidden until someone deploys without the aliases. Never treat "it still works" as proof.

**Bundle / consoles** — `databricks.yml`, `resources/**/*.yml` (job names, the app `name:`, descriptions,
UC Volume comments), `databricks-bi/*.lvdash.json` (one markdown title each),
`databricks-genie/*.geniespace.json` (two prose lines; keep the line-array shape and the id sort order,
and regenerate the `bt_digital_poc` rendering with `scripts/render_genie_space.py` rather than editing both)

**Docs** — `mkdocs.yml` (`site_name`, `site_author`, `copyright`, and switching `theme.icon.logo` to
`theme.logo: assets/logo.png`; there is deliberately **no** `favicon:` key — the generated logo is a
674x303 wordmark that would render as a squashed sliver in a square favicon slot), `docs/**/*.md`
prose, `docs/stylesheets/extra.css` (header comment; the `.fx-*` class names are identifiers and
stay), `README.md`, `HANDOFF_DOCS_HUB.md`. Then `python scripts/build_docs_reference.py`,
`python -m mkdocs build`, `python scripts/build_app_docs.py`.

> `AGENTS.md` / `CLAUDE.md` need **no** brand edit: their only `flowx` hits are the `flowx_testing/`
> directory path, which is a real path (and renaming it would collide with the existing
> `metaflow_testing/`). `docs/UC3|UC6|UC7/` are **gitignored derived copies** of `BT_Usecase/<UC>/docs/`
> — rebrand the `BT_Usecase/` source, never the `docs/` copy, or the edit is lost on the next
> `build_docs_reference.py` run. `docs/reference/**` is likewise generated: its residual `FlowX`
> strings come from `src/flowx/` docstrings, so fix those and regenerate.

**Agent skills** — `agent_skills/**`, then `python scripts/sync_agent_skill.py` (the `.claude/` copies are
byte-compared, including line endings — never hand-edit them). The top-level
`.claude/skills/flowx-onboarding/SKILL.md` is **not** covered by that script — its `REFS` map
(`scripts/sync_agent_skill.py`) syncs only `golden_specs.json`, `spec_reference.json`,
`common_pitfalls.md` and `framework_guide.md` — so it is a hand edit of its own. The **directory name**
`flowx-onboarding` is the skill's invocation name and is deliberately NOT renamed; see the
not-configurable section.

**Repo-side Python and notebooks**
- `scripts/bootstrap_workspace.py` — the module docstring is prose. Its `--help` description and
  operator banner already read `branding/branding.py` at runtime and need no edit.
- `scripts/cleanup_dead_code.py`, `scripts/mkdocs_hooks.py` — a printed banner and a docstring
- `notebooks/**` — 5 files carry the brand in headings and printed output
  (`00_seed_sample_data/02_seed_flowx_testing_data.py`, `01_setup/01_setup_control_tables.py`,
  `09_documentation/09_dataflow_documentation.py`, `uc3/simulator/01_delta_table_setup.py` and
  `02_stream_producer.py`). Notebooks run on Databricks where the repo root is not importable, so
  they carry literals.
- `onboarding_templates/onboarding_spec.schema.json` — the `title` shown to spec authors and by
  JSON-schema tooling. The `$id` (`urn:flowx:onboarding-spec-schema`) is an identifier and stays.

**Delivery, testing and app-config surfaces**
- `BT_Usecase/_shared/delivery/*.md` — 4 customer-facing delivery documents. (`BT_Usecase/<UC>/docs/`
  is the source of the gitignored `docs/UC*/` copies; rebrand the `BT_Usecase/` original.)
- `flowx_testing/TESTING_PLAN.md`, `README.md`, `STABILITY_TEST_PLAN.md`, `TESTING_STATUS.md`
- `databricks-app/_cfg.json` — carries a `"title"`. Producer unidentified and nothing in `server/`
  reads it; confirm whether it is a dead build artifact before editing an 85 KB single-line JSON.
- `resources/observability/dlt_observability_job.yml` — a job display name in a group currently
  commented out of `databricks.yml`'s `include:`
- `docs/v*_json_attribute_delta.json` — 6 files. These are **historical** per-release deltas; leaving
  them is usually correct, and they are recorded here only so a brand grep does not read them as a
  missed rename.

**Tests that assert the brand** are listed at the end of this section; they read the value from
`branding.json` rather than hardcoding it, so they follow a rebrand automatically.

**Python display strings inside `src/flowx/`** — these are display text even though the package name is
not: `control_plane/observability_views.py` (UC view/column `COMMENT` clauses),
`control_plane/ddl_definitions.py:403` (UC function comment — **keep braces escaped as `{{ }}`** inside
those DDL f-strings), `observability/dataflow_documenter.py` (generated-document headings),
`onboarding/agent_tools.py:1`.

**Tests that assert the brand** and will fail until updated in the same commit:
`databricks-app/tests/test_e2e_bugs.py:25`, `test_frontend_serving.py:16` and `:57`
(the last one reads the mkdocs-built docs site, so it needs the docs rebuild too).

---

## What is deliberately NOT configurable, and why

These are **runtime identity**, not branding. `branding.json` has no key for any of them, on purpose.
Changing one does not rebrand anything — it breaks something.

### The Python package `flowx` (`src/flowx/`, `pyproject.toml` `name = "flowx"`)
**Consequence: every import in the repo fails.** There are **394** `import flowx` / `from flowx...`
statements across `src/`, `tests/` and `notebooks/`. The name also reaches places a rename would not
follow: `mock.patch("flowx.lakeflow_framework...")` target strings, `sys.modules` registrations, and
`__module__` assertions — all of which raise `ModuleNotFoundError` at patch time rather than failing
a text match. It is also the wheel name that every deployed job and pipeline installs. The package
name is invisible to users; the brand is carried by display strings.

### The Unity Catalog name `flowx`
**Consequence: an orphaned live catalog.** The catalog holds real control tables, audit logs and
observability views, and the bundle documents that it is a **prerequisite created once per workspace**
and *cannot* be declared as a bundle resource — a `catalogs.*` resource was tried and removed the same
day. Renaming the variable does not migrate data; it points the framework at a catalog that does not
exist while the old one keeps holding every control row. A catalog migration is a deliberate, manual
data-movement project, not a branding edit.

### The schema `flowx_sample`
**Consequence: the sample suite desynchronises from the schema it provisions.** The name is a UC
identifier threaded through job keys, pipeline filenames, `/Volumes/<catalog>/flowx_sample/` paths and
test regexes. Renaming the files while the schema stays put leaves a confusing half-state — and the
filename regexes fail **silently** (zero samples matched) rather than erroring.

### Bundle target names (`hoonartek`, `metaflow_v7`, `bt_digital_poc`)
**Consequence: broken CLI authentication.** The target named `hoonartek` pairs with a CLI profile of the
same name; renaming the target breaks that pairing and every `databricks bundle` command against it.
Deployment state also lives under `.databricks/bundle/<target>/`, so a rename orphans it. The vendor
rebrand is `hoonartek` → `NRM Analytix` in *display text*; the target is not display text.

### Also not in scope (identifiers that merely look like brand)
`/Volumes/...` and `/Workspace/Shared/flowx/...` paths; the `_flowx_export_pulse` DLT dataset (renaming
it changes what a streaming flow reads from, which fails the next incremental update and forces a full
refresh); the `flowx_app` logger name; and `bundle.name: flowx`, which derives the deployment root path
for every resource — renaming it orphans all previously deployed state and needs an explicit decision,
not a config edit.

---

## Adding a new branded string

1. Add the key to `branding.json`.
2. If a **generated** file should carry it, extend `render_branding_js` / `render_branding_py` in
   `scripts/apply_branding.py`.
3. If a **hand-maintained** file should carry it, import the accessor and read it at runtime — do not
   paste the literal.
4. Run `python scripts/apply_branding.py`, then `npm run build` if the frontend was affected.
