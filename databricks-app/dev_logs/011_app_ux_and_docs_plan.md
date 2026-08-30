# 011 — App UX, storage paths, template folder, MkDocs & rich attribute help

> Plan only. Nothing implemented yet. Six requests, sequenced into four waves, plus one defect
> found while investigating.
>
> Baseline: Onboarding App v1.5.0 (React/Vite frontend, FastAPI backend).

---

## What I found (verified against the current code)

| Request | Current state | Verdict |
|---|---|---|
| 1 · Path entry for UC Volume / Workspace | `spec_storage.roots` in `config/index.json` are **fixed**. `files.py::_sanitize_path` confines every read/write under the resolved root. There is no way to name a path outside a configured root. | Confirmed — the capability does not exist |
| 2 · `Open…` not visible | `Shell.jsx:32` — a plain bordered div, styled **identically** to the `Index` button next to it, `font:600 11.5px`, no icon, no accent. It reads as a tertiary control. | Confirmed |
| 3 · "Show attributes not applicable" not working | The toggle exists (`Builder.jsx:944`) and `showInert` gates **three** distinct paths — a whole-section early return at `:693`, and per-field returns at `:712` and `:731`. A section suppressed at `:693` never reaches the field-level logic, so its fields can never be revealed. | Confirmed as a **likely** root cause — to be proven by a repro before fixing, not assumed |
| 4 · Templates from a user folder | Templates load from `settings.config_dir.parent / "templates"` — **inside the app bundle**. Adding one requires a redeploy. Worse, a **second copy** of the template list is hardcoded in `web/src/registry.js` (lines 340, 377…). | Confirmed — plus an unreported duplication |
| 5 · MkDocs from `/docs` | There is **no MkDocs anywhere** in the repo. `databricks-app/docs_site/index.html` is a hand-written **556-line single-page HTML** that re-implements the attribute reference — a second copy of `docs/00_master_reference_index.md` guaranteed to drift. `docs/13` is not in it at all. | Confirmed |
| 6 · Rich attribute detail on the right | The info drawer (`Shell.jsx:233`) is already a right-side slide-over showing description, hint, current value, allowed values, children, siblings, type/required/default/sample/path, and one framework doc link. | Partly exists — the **content** is thin, not the panel |

### 🐛 Defect found while investigating (not in your list)

**`FULL_SNAPSHOT_CDC` is completely missing from the app.**

The framework's `ALLOWED_INGESTION_CDC_STRATEGIES` has six entries including `FULL_SNAPSHOT_CDC`.
`web/src/registry.js`'s `CDC` array has six entries **but not that one** — it jumps from `SCD3`
to `FULL_SNAPSHOT_CDC_NO_PK`. And `isCdc()` (line 42), which gates every hash / surrogate-key /
comparison-column field, also omits it.

Two consequences:
1. **You cannot author a with-primary-key snapshot flow in the app at all.**
2. If you *import* an existing spec that uses it, `isCdc()` returns false, so
   `generate_hash_columns`, `generate_surrogate_key`, `columns_to_check` and friends are all
   wrongly hidden — and a round-trip save could drop them.

Small fix, high value. Folded into Wave 1.

---

## Wave 1 — Visibility & correctness fixes (small, independent)

### 1.1 Diagnose and fix "Show attributes not applicable" (request 3)

**Diagnose first.** The repo already has an esbuild-based headless harness (built for the v1.5.0
round-trip proof) that drives the real `Builder` component with no DOM. Reuse it:

* Render the registry against default values with `showInert:false` → record the rendered field set.
* Render again with `showInert:true` → record the set.
* Assert the second is a strict superset, and that its size equals the full registry inventory
  (219 attributes) for each phase.

That turns "not working" into a number — *N fields are still hidden with the toggle on* — and
names them, which is what a fix has to target.

**Prime suspect** (to be confirmed, not assumed): `Builder.jsx:693`'s
`if(!s.showInert) return null;` sits at **section** level. A section whose own predicate fails is
dropped whole, so the per-field reveal logic at `:712`/`:731` never runs for anything inside it.
The likely fix is to let a section render in a visibly-inert state (dimmed, with its reason)
rather than returning null, so its children reach the field-level branch.

**Also to check in the same pass:** inert fields render at `opacity:0.45` (`:261`) — if the
palette makes that near-invisible against the panel background, a field that *is* rendering can
still read as missing. Verify contrast, raise to ~0.6 with an explicit "not applicable" badge if
needed.

**Acceptance:** with the toggle on, every one of the 219 registry attributes is reachable in some
phase, each inert one carrying a visible reason.

### 1.2 Make `Open…` a primary action (request 2)

* Promote to a filled accent button with a folder icon, visually separated from `Index`.
* Add `Ctrl/Cmd + O`.
* Surface it on the **empty state** too — a builder with no flows should say
  *"Start from a template · Open an existing spec"*, not just show a bare `+`.

### 1.3 Add `FULL_SNAPSHOT_CDC` to the registry

Add the missing entry to `CDC` with its own explanation and `primary_keys required` note, and add
it to `isCdc()`. Then re-run the round-trip harness on a spec using it to confirm no attribute is
dropped.

---

## Wave 2 — Arbitrary paths and a user-owned template folder (shared plumbing)

These two share the same storage machinery, so 2.1 lands first and 2.2 builds on it.

### 2.1 Path entry for UC Volume and Workspace (request 1)

**UI.** In the Open dialog (and the Save destination), alongside the root picker, add a **path
field**:

```
  ○ Volume · onboarding specs      /Volumes/metaflow/metaflow/onboarding_specs/
  ○ Workspace · specs              /Workspace/Shared/metaflow/specs/
  ● Enter a path…                  [ /Volumes/metaflow/land/ref/specs/my_spec.json      ]
                                     └─ Browse ─┘  lists the directory if a folder is given
```

Typing a **directory** lists it; typing a **file** opens it. The app reads it through the
Databricks API exactly as it reads a configured root today.

**Backend.** No new Databricks client code is needed — `files.py` already dispatches on
`root.kind == "volume"` vs `"workspace"`, and both paths are just the Files API and the Workspace
API. The change is in path resolution:

* New config block, **opt-in**:

```jsonc
"spec_storage": {
  "allow_arbitrary_paths": true,
  "allowed_path_prefixes": ["/Volumes/", "/Workspace/"],
  "roots": [ ... unchanged ... ]
}
```

* `FileManager` gains `resolve_arbitrary_path(path)` → synthesises a root whose `kind` is derived
  from the prefix (`/Volumes/` → volume, `/Workspace/` → workspace) and whose `path` is the
  parent directory. `_sanitize_path` still runs, so `..` traversal is still rejected.
* `/api/storage/{list,read,write}` accept `path` **without** `root_id` when
  `allow_arbitrary_paths` is on.

> **Decision for you — I recommend the allowlist.** The alternative is unrestricted absolute
> paths. The prefix allowlist costs nothing in usability (both real storage kinds are covered)
> and keeps the app from being turned into a general workspace file browser by a typo. Say the
> word and I'll drop the allowlist instead.

**Write access stays governed separately.** A configured root can be `read: true, write: false`;
an arbitrary path inherits `write` from `allow_arbitrary_paths_write` (default **false**), so
"read a JSON from anywhere" does not silently become "overwrite anything".

### 2.2 Templates from a folder you control (request 4)

**Today:** templates live inside the deployed bundle. You cannot add one without a redeploy, and
there are two copies of the list (`templates/index.json` + hardcoded in `registry.js`).

**Plan:**

1. New config block, reusing the same root machinery:

```jsonc
"template_storage": {
  "roots": [
    { "id": "vol_templates", "label": "Volume · spec templates", "kind": "volume",
      "path": "/Volumes/{{catalog}}/metaflow/spec_templates/", "read": true, "write": true }
  ]
}
```

2. `GET /api/templates` returns **built-in ∪ folder**. Any `.json`/`.yaml` dropped in that folder
   appears in the browser with **no index file required** — v1.5.0 already derives the summary
   chips (source type, strategy, format, feature flags, attribute count) from the preset body, so
   a bare template file describes itself.
3. Optional `_index.json` in the folder, only to override labels, add descriptions, or pin
   entries. Absent → everything is auto-derived.
4. **Save as template** — a button in the builder that writes the current flow into that folder,
   so a user creates a template by building one, not by hand-writing JSON.
5. **Remove the duplicated template list from `registry.js`**, leaving `/api/templates` as the
   single source. (Keep a minimal built-in `blank` set as an offline fallback.)

Template files carrying `{{catalog}}` / `{{env}}` continue to resolve through the existing
`template_variables` substitution.

---

## Wave 3 — Real MkDocs site from `/docs` (request 5)

**Today:** `docs_site/index.html` is a hand-maintained 556-line HTML page duplicating the
attribute reference. It does not contain docs 01–13 at all, and it will drift from
`docs/00_master_reference_index.md` — it already has.

**Plan:**

1. **`mkdocs.yml` at the repo root**, `docs_dir: docs`, Material theme, search on, dark/light
   toggle matching the app's palette.
2. **Navigation** — grouped so a reader lands on the right page rather than scanning 14 files:

```yaml
nav:
  - Home: index.md                       # NEW — role-based entry points + FAQ
  - Start here:
      - Developer guide: 09_developer_guide_and_recipes.md
      - ⚠️ Known limitations & gotchas: 13_known_limitations_and_gotchas.md
      - Attribute reference: 00_master_reference_index.md
  - Building pipelines:
      - Ingestion & sources: 02_ingestion_and_sources.md
      - Transformation & CDC: 03_transformation_and_cdc.md
      - Data quality & governance: 04_data_quality_and_governance.md
      - Security & cryptography: 05_security_and_cryptography.md
      - Egress & sinks: 06_egress_and_lakeflow_sinks.md
      - Reconciliation: 07_reconciliation_engine.md
      - Observability: 08_observability_and_telemetry.md
  - Reference:
      - Platform architecture: 01_platform_architecture.md
      - Permutation matrix: 12_module_permutation_matrix.md
      - Hashing & determinism: 11_hashing_and_determinism.md
  - FAQs: 10_multi_role_faqs.md
```

3. **New `docs/index.md`** — the landing page you asked for: three role lanes (*I'm authoring a
   spec* / *I'm debugging a pipeline* / *I'm reviewing the architecture*), a "read this before
   filling in JSON" callout pointing at doc 13, and the search box.
4. **Build output → `databricks-app/docs_site/`.** The existing `/docs` StaticFiles mount in
   `server/app.py:146-148` then serves it unchanged — **no server code change at all**.
5. **Delete `docs_site/index.html`** (the legacy hand-written copy). Nothing else references it.
6. **Re-point `config/docs.json`.** MkDocs slugifies headings, so the current 17 anchors
   (`#1-top-level-spec-schema`, …) must be re-verified against the *generated* HTML, not assumed.
   I'll add a check that resolves every registry anchor against the built site and fails the
   build on a 404 — the v1.5.0 notes record that stale anchors already 404'd once.
7. **Build step.** `mkdocs build` must run before deploy, same constraint as `npm run build`.
   Documented in the app README and added to the deploy checklist. `mkdocs-material` is a
   **build-time** dependency only — the app serves static HTML, so nothing changes at runtime.

---

## Wave 4 — Rich per-attribute help in the right pane (request 6)

The panel already exists and slides in from the right. What's missing is **content**. This wave
adds a richer field-definition contract and fills it.

### 4.1 Extend the field definition shape

`registry.js` fields currently carry `i:` (one description string), `hint:`, `ph:` (placeholder),
`req:`, `d:` (default). Add optional keys:

| Key | Purpose |
|---|---|
| `why` | Why this attribute exists — the problem it solves, the reason behind it |
| `sample` | A real worked value, richer than the placeholder |
| `tips[]` | Practical guidance ("set this whenever the stream runs continuously") |
| `errors[]` | `{code, cause, fix}` — known error signatures this field produces |
| `limits[]` | Trap ids from `docs/13` (`"S1"`, `"C1"`, `"D1"`…) |
| `dbxDoc` | Official **Databricks** documentation URL, distinct from the framework doc link |

### 4.2 Source the content from `docs/13` — this is the payoff

[`docs/13_known_limitations_and_gotchas.md`](../../docs/13_known_limitations_and_gotchas.md)
already carries ~50 traps, each with a stable id, a severity grade, a symptom and a fix. Wiring
`limits:` to those ids means the drawer inherits all of it, and the doc stays the single source.

Initial mapping (illustrative, not exhaustive):

| Attribute | `limits` | What the user then sees in the drawer |
|---|---|---|
| `source_zip_handling.target_volume_path` | `S1` | 🔴 *"Must match `source_config.path`. If it doesn't: extraction succeeds, zero rows ingested, no error."* |
| `pre_extraction_decryption.secret_passphrase` | `S2` | 🔴 The three easily-confused secret fields, and which protects what |
| `source_zip_handling.zip_file_pattern` | `S3` | 🔵 Marker-collision → re-extraction on every update |
| `source_config.schema_config_path` | `C1`, `C7` | 🔴 Does **not** project — 4 declared of 50 still lands 50 |
| `source_config.column_normalization.*` | `C2`–`C5` | 🔴 Post-normalization naming rules; the `enabled`-omitted trap |
| `source_config.data_standardization_sql` | `C2` | 🔴 Runs last — write it against final column names |
| `target_config.cdc_load_strategy` (snapshot) | `D1`, `D4` | 🔴 Accumulation over a streaming upstream; deletes never happen |
| `target_config.empty_target_if_source_empty` | `D2` | 🟡 Accepted but **inert** (E09 withdrawn) |
| `target_config.partition_columns` | `D3` | 🟡 Silently ignored on every CDC strategy but APPEND/TRUNCATE_AND_LOAD |
| `target_config.surrogate_key_columns` | `D4` | 🔴 `[]` ≠ absent — an empty list raises, it does not mean "all columns" |
| `target_config.encrypted_columns` | `E1` | 🔴 The `secret(...)` redaction corruption |
| `dq_config.rules[].action` | `Q1` | 🟠 `fail` stops the **whole update** |
| `reconciliation_flows[].recon_mode` | `R5` | 🟠 `continuous` cannot run on serverless — leave unset |
| `target_configs[].append_target_table` | `R1` | 🔴 Append-only requirement when the target feeds a stream |
| `source_config.reader_options` | `A1`, `A2` | 🔴 Overwritten files ignored; `inferColumnTypes` defaults false |

### 4.3 Render

Collapsible sections in the existing drawer, in this order:

```
  <attribute name>                       [🔴 known limitation]
  ──────────────────────────────────────────────────────────
  What it does          (existing `i`)
  Why it's needed       (new `why`)                    ← the "reason behind" you asked for
  Sample value          (new `sample`, copyable)
  Current value         (existing)
  Type / required / default / json path   (existing)
  Allowed values        (existing, annotated chips)
  ▸ Tips                (new)
  ▸ Known errors        (new — code · cause · fix)
  ▸ Known limitations   (new — severity chip + summary + deep link into docs/13)
  ▸ Related attributes  (existing siblings)
  Docs:  [Framework ↗]  [Databricks ↗]                 ← two links, not one
```

A severity chip appears on the **field label itself** in the form when it carries a 🔴 trap, so
the user sees there is something to read before clicking.

### 4.4 Scope — tiered, because 219 attributes is a lot of prose

| Tier | Attributes | Content |
|---|---|---|
| **T1** | ~45 — every attribute that carries a `docs/13` trap, plus every `required` field | Full treatment: why · sample · tips · errors · limits · both doc links |
| **T2** | ~80 — everything with a non-trivial predicate or enum | why · sample · framework doc link |
| **T3** | remainder | Existing description + type row, unchanged |

T1 is where essentially all the value is, and it is the tier `docs/13` already writes for me.
I'd deliver T1 complete and T2/T3 incrementally.

---

## Sequencing & why

| Wave | Contents | Depends on | Rough size |
|---|---|---|---|
| **W1** | Inert-toggle fix · `Open…` prominence · `FULL_SNAPSHOT_CDC` | — | Small |
| **W2** | Arbitrary path entry → user template folder | shared storage plumbing, 2.1 before 2.2 | Medium |
| **W3** | MkDocs site + FAQ landing + anchor verification | — (independent) | Medium |
| **W4** | Rich attribute help, tier 1 | **W3** — the drawer's doc links must resolve first | Large |

W1 first because it is bug-fixing and cheap. W4 last because its links point into the site W3
builds.

---

## Three decisions I need from you

1. **Arbitrary paths — allowlisted or unrestricted?** I recommend the `/Volumes/` + `/Workspace/`
   prefix allowlist (§2.1): no usability cost, keeps the app from becoming a general file browser.
2. **Template folder location — Volume, Workspace, or both?** I recommend **both**, reusing the
   `spec_storage` root machinery so it is one mechanism, not two.
3. **Attribute-help scope — tier 1 (~45) now, or all 219?** I recommend tier 1 first: it covers
   every trapped and every required attribute, and you can judge the format before I write ~170
   more entries.

## Verification for each wave

* `pytest databricks-app/tests` stays green (60 tests today).
* Round-trip harness: load a canonical spec → re-serialise → **0 diffs**, re-run after every
  registry change (W1.3 and W4 both touch `registry.js`).
* W1: headless render count with `showInert` on/off — every one of 219 attributes reachable.
* W3: automated anchor check — every registry anchor resolves against the built site, build fails
  on a 404.
* W2: `/api/storage/{list,read,write}` exercised against an arbitrary path and a configured root,
  plus a traversal-rejection case.
* `databricks bundle validate -t dev_metaflow` before any deploy.
* `RELEASE_NOTES.md` entry per wave.

---

## ⚠️ v1.4.0 update — read before acting on anything above

> Appended 2026-08-30. **The plan text above is left exactly as written**; it is a historical
> record of what was true at Onboarding App v1.5.0. Several attributes it names no longer
> exist in the framework, so parts of it are now unimplementable as literally specified.

Removed in v1.4.0 — **rejected at onboarding, not ignored**, so any of these still present in
`registry.js` or in a stored spec is a hard failure, not a hidden field:

| Referenced above | Where | Status in v1.4.0 |
|---|---|---|
| `FULL_SNAPSHOT_CDC_NO_PK` | the "completely missing from the app" defect, §"What I found" | **Strategy removed.** Snapshot CDC is now `FULL_SNAPSHOT_CDC` + real `target_config.primary_keys`, dispatched to the Databricks-native `dlt.apply_changes_from_snapshot`. A keyless source belongs on `TRUNCATE_AND_LOAD`. `ALLOWED_INGESTION_CDC_STRATEGIES` therefore has **five** entries, not six |
| `generate_surrogate_key` | the `isCdc()` consequence list | **Removed.** The whole surrogate-key engine is gone (`crypto/hashing.py` deleted, `__framework_surrogate_key` no longer generated) |
| `target_config.surrogate_key_columns` | trap table `D4` | **Removed** along with `surrogate_key_exclude_columns` and `reconciliation_flows[].generate_surrogate_key`. Trap `D4` no longer applies |
| `reconciliation_flows[].recon_mode` | trap table `R5` | **Removed** (both values). Reconciliation is triggered-only — batch reads, `trigger(availableNow=True)` on a streaming side, every run drains and stops. To reconcile more often, schedule the job more often. Trap `R5` no longer applies |

Still valid, but note the surrounding detail has moved on:

* The **underlying defect is real and still worth fixing** — `FULL_SNAPSHOT_CDC` was, and as of
  this note may still be, absent from `registry.js`'s `CDC` array and from `isCdc()`. Wave 1
  should add `FULL_SNAPSHOT_CDC` (with `primary_keys` **required**) and drop
  `FULL_SNAPSHOT_CDC_NO_PK` entirely rather than add both.
* `target_config.encrypted_columns` gained an optional `source_data_type` — the column's original
  Spark type before encryption (`"string"`, `"decimal(18,2)"`). Any attribute-help work (W4)
  should cover it.
* The `219 attributes` count and the tier-1 `~45` scope are both pre-v1.4.0 figures; re-derive
  them from `spec_validator.py` before committing to either.
