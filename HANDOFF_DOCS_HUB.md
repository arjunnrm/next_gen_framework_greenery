# HANDOFF — Metaflow documentation hub rebuild (v1.7.13 docs release)

**Written 2026-09-14 20:45 IST on branch `feature/0.0.4`.** This note lets a fresh session (any
account) continue the work without re-deriving it. Read it top to bottom once; then work the
**TODO** list in order. Delete this file in the commit that finishes the work.

> **FINAL (2026-09-15): all TODO items are complete — 188/188 attributes carry FAQs (773), the living tree builds warning-free, `databricks-app/docs_site/` is rebuilt. This file is retained only for §6 / Appendix C (the `heal_trigger` persistence defect and its patch); delete it once that fix lands.**
>
> Earlier status update: TODO items 2, 3, 4, 5, 6, 7 and 9
> are **done** (console pages, registry-only tree leaves, `reference/sync.md`, all living-page warnings fixed,
> docs/17 counts, the heal_trigger caveat, SKILL.md §14 + sync, docs/README rows, enhancement log
> `v1.7.13`, RELEASE_NOTES entry). `mkdocs build` is warning-free outside `archive/`;
> `tests/unit/test_docs_hub.py` 28 passed. **Open:** TODO 1 (FAQs: g1, g5, g6 merged = 80 of 188
> attributes; g2, g3, g4 were running on Sonnet subagents when this note was written — check
> `scratch`-equivalent outputs or re-run Appendix A/B) and TODO 8's final `python scripts/build_app_docs.py`.

---

## 1. The brief, in one paragraph

Transform the existing MkDocs repository into a Material for MkDocs developer hub: a
help-centre style landing page (topic directory + side panels, like a Nimble AMS help home),
component cards and a Mermaid architecture flow; four **pillar** deep-dive pages (Ingestion,
Transformation, Reconciliation, Observability) with content tabs, admonitions, badges and
Mermaid; a **console** section walking every real UI surface tab by tab (Spec Builder app,
Observability dashboard, Control-metadata dashboard, Genie space, agent skills); and a
**master configuration reference** generated from the registry/schema/validator with a
collapsible JSON tree, per-attribute type/constraints, JSON + validate + CLI + SQL tabs,
version/required badges and **at least four FAQs per attribute** (188 attributes). Plus the
sync story: pre-commit hooks, a CI workflow, an MkDocs hook that regenerates the reference on
build. Style: sharp Indian-English technical prose, zero fluff, no em-dashes in new prose.

Facts the brief got wrong, and how the pages handle them (keep this honest):

- Parameterised SQL is `${param}` from root `pipeline_parameters`, resolved on every update.
  Colon-style `:start_date` is **not** a feature. Reconciliation pillar says so and shows the
  real recipe (edit `pipeline_parameters` → onboarding `action_type=UPDATE` → run the job).
- There is no "Agent Console" tab. Agent skills are files + tool specs + Genie. The console
  page says so plainly and maps the requested tabs onto the real surfaces.
- Metaflow has no SaaS "cloud-to-cloud connectors"; sources are Auto Loader files, Zerobus tables,
  ASN.1 files. Ingestion pillar states this.
- Version badges use real framework versions (v1.4.0 … v1.7.5) from `docs/v*_json_attribute_delta.json`.

## 2. State of the working tree

Run `git status --short` first. Expected, all uncommitted:

| Status | Path | What |
|---|---|---|
| M | `mkdocs.yml` | Full rewrite: Material features, mermaid fence, `font: false`, `extra_css`, `hooks`, 9-tab nav (Home · Spec Builder app · Get started · Pillars · Console · Configuration · Architecture · Code reference · Help · Project record). |
| M | `docs/index.md` | Help-centre directory landing page (`.fx-directory` grid, 8 topics, 3 side panels), component cards, Mermaid architecture flow, pillars table. |
| M | `scripts/build_docs_reference.py` | Extended generator (see §3). Backup of the previous version: scratchpad only; `git show HEAD:scripts/build_docs_reference.py` is the authoritative old copy. |
| M | `scripts/sync_agent_skill.py` | New `--check` flag (exit 1 on stale copies, writes nothing). |
| M | `docs/reference/json/*.md`, `docs/reference/code/index.md` | Regenerated output. Never hand-edit. |
| ?? | `docs/stylesheets/extra.css` | `.fx-badge` pills (`fx-req fx-opt fx-ver fx-dep fx-only fx-prod fx-flow`), `.fx-directory/.fx-topic/.fx-panel`, `.fx-tree`, `.fx-mock`, `.fx-hero`. |
| ?? | `scripts/mkdocs_hooks.py` | `on_startup`: runs the generator on `mkdocs build` only (never `serve`, to avoid a rebuild loop); `FLOWX_DOCS_SKIP_REGEN=1` skips. `on_config`: exposes `extra.flowx_version`. |
| ?? | `scripts/check_docs_warnings.py` | Builds and fails on any WARNING whose source page is outside `docs/archive/`. CI gate. |
| ?? | `docs/requirements.txt` | Pinned toolchain (mkdocs 1.6.1, material 9.7.7, pymdownx 11.0.2). |
| ?? | `.pre-commit-config.yaml` | check-json/check-yaml + four local `--check` hooks. |
| ?? | `.github/workflows/docs.yml` | The four CI gates + zero-warning build. |
| ?? | `databricks-app/config/attribute_faqs.json` | **Skeleton only — `attributes: {}`.** The FAQ content is the biggest open item (§4 TODO 1). |
| ?? | `docs/reference/json/tree.md`, `removed.md` | New generated pages. |
| ?? | `docs/pillars/index.md` | Hand-written hub. |
| ?? | `docs/pillars/{ingestion,transformation,reconciliation,observability}.md` | Written by subagents (622 / 722 / 597 / 501 lines). Structure verified complete; JSON samples validated (see §5). **Needs a human-quality read for tone and anchor warnings after `mkdocs build`.** |
| ?? | `tests/unit/test_attribute_faqs.py` | Fails until FAQs exist (by design). |
| ?? | `tests/unit/test_docs_hub.py` | Nav/hook/generated-page/anchor guards. Some tests fail until the console pages exist. |
| ?? | `scratch/heal_trigger_persistence.patch` | See §6 defect. `scratch/` is gitignored. |

**Not yet created:** `docs/console/{index,spec_builder,observability_dashboard,control_dashboard,genie,agent_skills}.md`, `docs/reference/sync.md`, `enhancement_logs/v1.7.13_enhancement_log.md`, the `RELEASE_NOTES.md` entry.

## 3. How the generated reference works now (`scripts/build_docs_reference.py`)

Inputs → outputs, all under `docs/reference/json/`:

| Output | Derived from |
|---|---|
| `index.md` | Master reference landing: cards, badge legend, sections table with FAQ counts, verification-workflow Mermaid, CDC table (+ removed strategies), "Recently added attributes" (from the delta files). |
| `tree.md` | `onboarding_spec.schema.json` → nested `<details>` HTML; each leaf links `../<page>/#<anchor>` (directory URLs, because raw-HTML links are not rewritten by MkDocs). Attributes present only in the registry (no schema node) are the `unlinked` list in §5; TODO 3 adds them as "registry-only" leaves. |
| `removed.md` | `spec_validator.py` dict literals via `ast`: `REMOVED_*_KEYS`, `REMOVED_CDC_LOAD_STRATEGIES`, `REMOVED_MATERIALIZE_POLICIES` (text from `engine/source_plane.py::MATERIALIZE_NEVER_REJECTION`), `REMOVED_RECONCILIATION_DATASET_KEYS_PIPELINE`, `RECONCILIATION_FLOW_KEYS_REQUIRING_PIPELINE_MODE`, `UNKNOWN_KEY_ALIASES`. |
| `root.md … observability.md` | Per attribute: badges (Required/Optional · flow · `vX.Y+` · Spec Builder section), purpose/why, **Type & constraints** table (schema type/default/enum/pattern), **Persisted in** `config.<table>.<column>` (see `persisted_in()`), four tabs (`JSON` · `Validate offline` · `Onboard (CLI)` · `Verify (SQL)`), tips, known errors, FAQs as `??? question "<kind> · <q>"`, **See also** (pillar · attribute dictionary anchor resolved against the real headings of `00_master_reference_index.md` · schema tree · console pages · gotchas). |

Anchor rule (shared with `scripts/build_app_docs.py::anchor`, do not change):
`path.replace("@","").replace(".","").replace("[]","").replace("_","-").lower()`.

**Anchor contract the generator imposes on the console pages** (`FLOW_LINKS` in the script;
`tests/unit/test_docs_hub.py::test_generator_console_and_pillar_links_resolve` enforces it).
The console pages must contain headings whose MkDocs slugs are exactly:

| Page | Required heading slugs |
|---|---|
| `docs/console/spec_builder.md` | `ingestion-tab`, `transformation-tab`, `reconciliation-tab`, `spec-root-and-observability` |
| `docs/console/control_dashboard.md` | `ingestion-flows`, `transformation-flows`, `reconciliation`, `observability-audit` |
| `docs/console/observability_dashboard.md` | `data-flow-throughput`, `framework-lineage`, `quality-reconciliation` |
| `docs/pillars/transformation.md` | `load-strategies` (already present) |

Compute a slug with `python -c "from markdown.extensions.toc import slugify; print(slugify('Quality & Reconciliation','-'))"`.

## 4. TODO, in order

1. **FAQs — 188 attributes × ≥4** (`databricks-app/config/attribute_faqs.json` → `attributes`).
   Shape per attribute: `[{"kind": "omitted|format|performance|edge_case", "q": "...?", "a": "..."}]`,
   all four kinds present, answers ≤120 words, no headings, grounded in
   `spec_validator.py` / schema / `docs/*.md` / runtime modules; never invent defaults or versions.
   `tests/unit/test_attribute_faqs.py` is the acceptance test. Two ways to produce them:
   - **Subagents (what was attempted):** six groups, one agent each, each writing its own JSON to
     a scratch path, then merged. Every launch so far died on the account's session rate limit
     (429 "session limit"), so launch **at most two at a time** and merge as they land. Regenerate
     the group files with the script in Appendix A (`scratch/faq/g{1..6}_attributes.json` +
     `all_attribute_paths.txt`), then use the brief in Appendix B verbatim, substituting the group
     file and output path. An agent left a partial `build_g6_faqs.py` (2 of 29 attributes) in the
     previous session's scratchpad; ignore it.
   - **By hand / in-session:** same shape, same rules; the group files list type, required, flows,
     section, schema default/enum, purpose, why, sample, tips and errors for each attribute.
   Merge: `attributes = {**g1, **g2, ..., **g6}`; run `pytest tests/unit/test_attribute_faqs.py -q`;
   then `python scripts/build_docs_reference.py`.
2. **Console pages** (`docs/console/`), hand-written, honouring the anchor contract in §3:
   - `index.md`: what the console is (Spec Builder app + two AI/BI dashboards + Genie + agent tools),
     a mapping table from the brief's requested tabs (Pipeline Operations / DQ & Recon / Telemetry &
     Logs / Agent Console) to the real surfaces, deployment commands (`bundle deploy` then
     `bundle run flowx_onboarding_app` — the app does not pick up new code on deploy alone;
     dashboards via `resources/flowx_bi/*.yml` with `dataset_catalog`/`dataset_schema`; Genie via
     `resources/flowx_genie/`, catalog baked in per target by `scripts/render_genie_space.py`).
   - `spec_builder.md`: header tabs Ingestion · Transformation · Reconciliation · Observability;
     phases from `databricks-app/config/phases/*.json` (ingestion: Identity, Source, Reader, Load
     strategy, Protect; transformation: Identity, Inputs, Transform, Load strategy, Protect;
     reconciliation: Identity, Datasets, Matching, Quality; spec: Spec root, Observability); left
     rail (Access, Sections, "Show attributes not applicable"), right rail live JSON/YAML preview +
     `raw`, attribute inspector (**i**), Open spec (upload / Volume / Workspace, Browse, Validate),
     Save (Volume / Workspace / download; JSON, YAML, both), templates (`databricks-app/templates/`),
     run onboarding (stages: Upload & stage spec → Validate → Upsert control table metadata → Register
     datasets → Apply governance tags → completed; job trigger via OBO). Server API routes exist at
     `/api/...`: `config`, `attribute-knowledge`, `templates`, `validate`, `import`, `render`,
     `write`, `list`, `read`, `resolve`, `predicate`, `diff`, `runs/{id}`, `{action}/run`. Use
     `.fx-mock` boxes for mock previews; per-tab action checklists.
   - `observability_dashboard.md`: `databricks-bi/flowx_observability_dashboard.lvdash.json`, **10
     pages**: Overview, Pipeline Performance, Data Flow & Throughput, Quality & Reconciliation, Cost &
     Efficiency, Framework & Lineage, AI Forecast, Job Orchestration, Zerobus Streaming, Global
     Filters (15 datasets). Widget titles per page were dumped in the previous session; re-dump with
     a 10-line python over the JSON (`pages[].layout[].widget.spec.frame.title`). Include the
     `dataset_schema: observability` note and the "forecast datasets are not filter-bound" rule from
     `docs/17` §5–6.
   - `control_dashboard.md`: `databricks-bi/flowx_control_metadata_dashboard.lvdash.json`, pages:
     Overview (Total Flows, Active Groups, Groups Behind, Target Wheel, wheel drift table), Ingestion
     Flows, Transformation Flows, Reconciliation, Observability & Audit, Raw JSON, Global Filters.
     Heading text must produce the slugs in §3 (e.g. `## Ingestion Flows`, `## Observability & Audit`).
   - `genie.md`: `databricks-genie/flowx_observability.geniespace.json`: 15 data sources (11 views +
     `config.onboarding_audit_log`, `config.reconciliation_mismatch_log`, `system.billing.usage`,
     `system.billing.list_prices`), 12 example question/SQL pairs, 12 sample questions (list them),
     the three serialized-format rules (version 2, line arrays, sorted blocks), the round-trip
     `databricks bundle generate genie-space --resource flowx_observability_genie_space --force`.
   - `agent_skills.md`: what exists (`agent_skills/SKILL.md`, `governance/SKILL.md`,
     `tool_specifications.json` → `validate_json`, `onboard_entity`, `get_catalog_schema_parameters`;
     `dlt_observability_tools.json` → `validate_observability_config`,
     `generate_pipeline_onboarding_config`, `diagnose_pipeline_telemetry_failures`; the discoverable
     `.claude/skills/flowx-onboarding/` copy kept in sync by `scripts/sync_agent_skill.py`), the
     non-negotiable validate loop, and an explicit "there is no Agent Console tab; execution history
     is the onboarding_audit_log + job runs" statement.
3. **Generator: registry-only tree leaves.** In `TreeRenderer.render`, after emitting a container's
   schema children, append knowledge attributes whose parent resolves to that container but which
   have no schema node (`@observability[].destination_config.*`, `delete_source_after_extract.*`,
   `target_configs[].target_catalog/schema/table`, `decrypted_columns[].input_name`,
   `source_config.explode_mode`, `target_config.partition_mode`) as `<li>` with a "registry-only ·
   validated by spec_validator" note. Then shrink the `allowed` set in
   `tests/unit/test_docs_hub.py::test_tree_view_links_every_documented_attribute` to
   `{"@observability_enabled"}`.
4. **`docs/reference/sync.md`** — "Docs ↔ code synchronisation": the derivation graph
   (`registry.js` → `build_attribute_knowledge.py` → `attribute_knowledge.json` (+ curated + FAQs) →
   `build_docs_reference.py` → `docs/reference/json/**` and `build_app_docs.py` → `docs_index.json`
   + `databricks-app/docs_site/`), the schema ↔ validator allowlist equivalence
   (`tests/unit/test_unknown_key_rejection.py::test_allowed_key_sets_match_json_schema`), the spec →
   control-table column map (the `persisted_in()` rules; the eight control tables in
   `control_plane/ddl_definitions.py`), Pydantic honesty (the framework spec is JSON Schema + a
   hand-written validator + frozen dataclasses; Pydantic is used only for the app's
   `server/settings.py`), the docstring convention (first sentence is the summary the AST reference
   shows), the MkDocs hook, pre-commit hooks, the CI workflow, and the `--check` commands.
5. **Fix the 11 pre-existing non-archive MkDocs warnings** (broken anchors) so
   `python scripts/check_docs_warnings.py` passes: `00_master_reference_index.md`
   (`#4-target-config--cdc-reference`, `#6-governance--tagging`, `#11-template-variables--parameter-substitution`,
   `#82-per-side-dataset-fields-source_config--each-target_configs-entry`, link into `02` §6),
   `01_platform_architecture.md` and `09_developer_guide_and_recipes.md` (link into `12` §4.2),
   `07_reconciliation_engine.md` (`#1110-…`, `#119-…`), `09` (`#d3--onboard-…`), `13` (link into `07` §11.10).
   Rule: MkDocs' slugifier collapses `--` to `-`; fix the links, not the headings. Then fix any new
   warnings from the pillar/console pages.
6. **Docs/17 drift:** §5 says 9 pages / 13 datasets; the dashboard has 10 / 15 (Zerobus Streaming
   page added). Update `docs/17_framework_observability_and_genie.md` §5 and §1 where it counts.
7. **Add a `!!! bug` admonition to `docs/pillars/reconciliation.md` § "Heal trigger …"** describing
   the §6 defect until it is fixed in the framework.
8. **Run everything:** `python scripts/build_docs_reference.py` · `pytest tests/unit/test_docs_hub.py
   tests/unit/test_attribute_faqs.py tests/unit/test_agent_skill_layout.py -q` ·
   `python scripts/check_docs_warnings.py` · `python scripts/build_app_docs.py` (rebuilds
   `databricks-app/docs_site/` and `config/docs_index.json`; the app serves the wiki at `/docs`).
9. **Definition of Done paperwork** (AGENTS.md steps 6–9; steps 1–5 do not apply — no spec attribute
   changed): `agent_skills/SKILL.md` §14 doc index gains the new pages → `python
   scripts/sync_agent_skill.py`; `docs/README.md` map gains rows; new
   `enhancement_logs/v1.7.13_enhancement_log.md` (scope table, previous vs current, impacted
   assets, verification, **defects found: §6**, known gaps: whatever FAQs remain);
   newest-first entry in `RELEASE_NOTES.md` (`pyproject.toml` stays `0.0.7`).
10. Delete this file; commit with the attribution trailer the session requires.

## 5. Verification already done

- `python scripts/build_docs_reference.py` exits 0; tree links 171 of 188 attributes (the 17
  unlinked are registry-only, TODO 3).
- Pillar page JSON samples: every fragment wrapped into a minimal spec passes
  `validate_spec(None, spec)` **except where noted in the session log below** (re-run
  `scratch`-style script: wrap `reconciliation_id` objects into `reconciliation_flows`, `flow_step_id`
  into `transformation_flows`, `dataflow_id`+`source_type` into `ingestion_flows`, destinations into
  `observability`, always with golden spec 01's ingestion flow alongside).
- `mkdocs build` baseline before this work: 231 warnings, 11 outside `archive/`.

## 6. Defect found (framework, out of scope here — do not lose it)

`heal_trigger` (`reconciliation_flows[].heal_trigger`, enum `source_stream | update_pulse`) is in
the JSON schema, accepted by `spec_validator.py`, and **read from the control row at runtime**
(`engine/source_plane.py:488`, `reconciliation/graph_registration.py:421` via `_row_get(row,
"heal_trigger")`), but the committed `onboarding/metadata_upsert.py` never writes it and
`control_plane/ddl_definitions.py` has no `heal_trigger` column (neither in the CREATE TABLE nor in
`ADDITIVE_CONTROL_TABLE_COLUMNS`). Result: `heal_trigger: "update_pulse"` onboards cleanly and is
silently read back as `source_stream`. A subagent fixed this during the previous session (adding the
column, the additive-migration entry and the `StructField` + `Row` field); the fix was reverted
from this docs change and saved as `scratch/heal_trigger_persistence.patch` (gitignored, on this
machine) and reproduced in Appendix C. Land it as its own change with a unit test
(`tests/unit/test_recon_heal_trigger_update_pulse.py` exists for the runtime side; add the upsert
round-trip) and a `docs/v0.0.8_json_attribute_delta.json` entry.

## 7. Session limits — why this handoff exists

Ten parallel subagents (six FAQ groups + four pillar pages) were launched twice; both times the
account's session rate limit (HTTP 429) killed most of them mid-flight. The four pillar pages
survived because each agent wrote its file before validating. Launch at most two agents at a time,
or write in-session.

---

## Appendix A — regenerate the FAQ group files

```python
# scratch/make_faq_groups.py  (run from the repo root; writes scratch/faq/)
import json, os
os.makedirs("scratch/faq", exist_ok=True)
kb = json.load(open("databricks-app/config/attribute_knowledge.json", encoding="utf-8"))["attributes"]
schema = json.load(open("onboarding_templates/onboarding_spec.schema.json", encoding="utf-8"))
defs = schema["$defs"]
def resolve(n):
    while "$ref" in n: n = defs[n["$ref"].split("/")[-1]]
    return n
def lookup(path):
    parts = [x for x in path.lstrip("@").replace("[]", "").split(".") if x]
    roots = [schema] + [resolve(defs[d]) for d in ("ingestionFlow", "transformationFlow", "reconciliationFlow", "observabilityDestination")]
    for root in roots:
        node, ok = root, True
        for part in parts:
            node = resolve(node)
            if node.get("type") == "array" and "items" in node: node = resolve(node["items"])
            props = node.get("properties", {})
            if part in props: node = props[part]
            else: ok = False; break
        if ok:
            node = resolve(node)
            return {"type": node.get("type"), "enum": node.get("enum"), "default": node.get("default")}
    return None
groups = {f"g{i}": [] for i in range(1, 7)}
for p, e in sorted(kb.items()):
    m = e["_meta"]; flows = m.get("flows", [])
    if flows == ["reconciliation"]: g = "g6"
    elif "root" in flows or p.lstrip("@").startswith("observability"): g = "g1"
    elif p.startswith(("source_config.source_zip_handling", "target_config.encrypted_columns", "decrypted_columns", "source_inputs")) or p in ("flow_step_id", "transformation_sql"): g = "g3"
    elif p.startswith("target_config.sink_config"): g = "g5"
    elif p.startswith(("target_config.", "dq_config", "governance_tags")): g = "g4"
    else: g = "g2"
    groups[g].append({"path": p, "type": m.get("type"), "required": m.get("required"), "flows": flows,
        "section": m.get("section"), "schema": lookup(p), "purpose": e.get("purpose", ""), "why": e.get("why", ""),
        "sample": (e.get("samples") or [{}])[0].get("code", ""), "tips": e.get("tips", []), "errors": e.get("errors", [])})
for g, items in groups.items():
    json.dump(items, open(f"scratch/faq/{g}_attributes.json", "w", encoding="utf-8"), indent=1, ensure_ascii=False)
    print(g, len(items))
open("scratch/faq/all_attribute_paths.txt", "w", encoding="utf-8").write("\n".join(sorted(kb)))
```

Expected sizes: g1 21 (root + observability), g2 39 (ingestion flow-level + source_config core),
g3 42 (zip/PGP, encrypted/decrypted columns, source_inputs, flow_step_id, transformation_sql),
g4 27 (target_config core, dq_config, governance_tags), g5 30 (sink_config), g6 29 (reconciliation).

## Appendix B — FAQ agent brief (use verbatim; substitute GROUP and OUTPUT)

> You are a senior technical writer for Metaflow, a metadata-driven Databricks Lakeflow framework in
> the repo at C:\Databricks\NextGen_Metadata_Framework. Author grounded FAQs for a group of
> onboarding-spec attributes. Do NOT modify any file inside the repo. Write ONLY to OUTPUT.
>
> INPUT: GROUP (each entry: path, type, required, flows, section, schema {type/enum/default},
> purpose, why, sample, tips, errors) and `scratch/faq/all_attribute_paths.txt` (anything you name as
> an attribute must appear there, or be a framework-generated column named in docs).
>
> GROUND TRUTH (grep by attribute name): `src/flowx/lakeflow_framework/onboarding/spec_validator.py`;
> `onboarding_templates/onboarding_spec.schema.json`; the relevant `docs/0x_*.md` (02 ingestion, 03
> transformation/CDC, 04 DQ/governance, 05 crypto, 06 sinks, 07 reconciliation, 08 observability),
> `docs/00_master_reference_index.md`, `docs/13_known_limitations_and_gotchas.md`,
> `docs/14_onboarding_restrictions_and_validation_rules.md`, `agent_skills/reference/common_pitfalls.md`,
> `agent_skills/reference/module_map.md`, the runtime module that reads the attribute, and
> `docs/v*_json_attribute_delta.json` (only cite a version found there or in docs).
>
> OUTPUT JSON shape: `{"<exact path>": [{"kind": "omitted|format|performance|edge_case", "q": "...?", "a": "..."}]}`.
> Rules: every path in GROUP is a key; ≥4 FAQs each with all four kinds present ("omitted" = behaviour
> when absent: default / required-error text / inert; "format" = quoting, case, shape, prefix,
> date, three-part-name gotchas; "performance" = cost/runtime impact, say "negligible" and why when
> true; "edge_case" = interplay with another real attribute, mode, strategy or platform rule). Every
> claim traceable to a line you read; never invent defaults, versions, limits or messages; quote
> validator rejection text verbatim in single quotes when relevant; when the validator does not
> check something say "not validated at onboarding — verify at runtime". Answers 1–4 sentences, ≤80
> words, Indian-English technical style, no marketing, attribute/column names in backticks, optional
> pointer "See docs/07 §11." only to a heading that exists. Questions phrased as a user would ask,
> specific to the attribute, not reused across attributes. Valid UTF-8 JSON, ASCII quotes in prose,
> no markdown headings in answers.
>
> Reply with: attributes covered, total FAQs, attributes with thin grounding and what you did.

## Appendix C — the reverted `heal_trigger` persistence fix (reproduce in its own change)

`control_plane/ddl_definitions.py`, `get_reconciliation_flow_spec_ddl`, after `execution_mode`:

```
    heal_trigger            STRING COMMENT 'source_stream (default when NULL) | update_pulse. Only meaningful when execution_mode is pipeline -- the only mode with an L5 heal lane. "source_stream" STREAMS the reconciliation source to drive the heal append_flow, so the source must be append-only (a MERGE-written or TRUNCATE_AND_LOAD/materialized_view producer is rejected by the G-STREAM plan guard and by V-CYC-7 at onboarding). "update_pulse" decouples the trigger from the payload exactly as sink_config.export_trigger per_update does for exports: a rate-micro-batch pulse carrying no data drives a declarative dlt.create_sink(format=delta) + @dlt.append_flow while the miss set joins in as a batch dlt.read, so nothing streams the source and a non-append-only source can heal in-graph.',
```

`ADDITIVE_CONTROL_TABLE_COLUMNS["reconciliation_flow_spec"]`, after the `execution_mode` tuple:

```python
        (
            "heal_trigger",
            "STRING",
            "source_stream (default when NULL) | update_pulse. What drives the L5 heal "
            "append_flow under execution_mode pipeline: streaming the reconciliation source "
            "itself, or the update-scoped rate-micro-batch pulse that lets a non-append-only "
            "source heal in-graph.",
        ),
```

`onboarding/metadata_upsert.py`: add `StructField("heal_trigger", StringType(), nullable=True)` after
`execution_mode` in `_RECONCILIATION_FLOW_SPEC_SCHEMA`, and `heal_trigger=flow.get("heal_trigger"),`
after `execution_mode=flow.get("execution_mode"),` in the `Row(...)` literal of
`upsert_reconciliation_flow_spec`. Braces in DDL COMMENT strings must be `{{ }}` (none needed here).
