# Declarative Automation Bundles Project

This project uses Declarative Automation Bundles (DABs) for deployment. Add project-specific instructions below.

## For AI Agents: Use Databricks AI Tools

**BEFORE any other action, read the `databricks-core` skill.**

It sets you up to work with this project reliably: CLI authentication, profile
selection, data discovery, and the bundle deployment workflow. Without it,
results are often slower and less accurate.

If this skill is not available (Databricks AI Tools are not installed), you can install them for your coding agent in seconds:

```bash
databricks aitools install
```

If the CLI is not installed, see: https://docs.databricks.com/dev-tools/cli/install

---

## Project Instructions

### Definition of Done for any framework change

A change to the framework is **not complete** when the code works. It is complete when every
artefact that *describes* the code has been brought back into agreement with it. A change that
updates the code but not the rest leaves the repo describing a system that no longer exists —
which is worse than not documenting it at all, because the stale description is trusted.

After implementing any change, work through all nine steps below. Do them in this order — each one
is easier once the previous is settled:

| # | Step | What "done" means |
|---|---|---|
| 1 | **Testing** | Add/update unit tests for the new behaviour (and, for a *removal*, a test asserting the removed thing is genuinely gone/rejected). Run `pytest tests/unit`, `pytest databricks-app/tests` (from `databricks-app/`), and `databricks bundle validate -t <target>`. Record the pre-existing-failure baseline so you can prove you added none. |
| 2 | **Python validation** | Update `onboarding/spec_validator.py`. A new attribute needs a check; a **removed** attribute needs an explicit rejection with a migration message — never silent ignoring (see "Removals" below). |
| 3 | **JSON schema + templates** | `onboarding_templates/onboarding_spec.schema.json`, plus `pipeline_onboarding_template.{json,yaml}` (these two are asserted equivalent by `test_spec_loader.py` — change both), `onboarding_spec_full_reference.{json,md}`, and any affected `flowx_testing/*.json`. |
| 4 | **JSON change details for the app** | Produce/extend a machine-readable attribute delta (`docs/vX.Y.Z_json_attribute_delta.json`) listing every added/modified/removed attribute with type, default, verbatim rejection message, migration and the concrete UI action it implies. This is what an automated agent updating the Databricks App consumes — write it for a machine, not a human. |
| 5 | **Databricks App** | `databricks-app/config/registry/*.json`, `config/attribute_knowledge*.json`, `config/validation/rules.json`, `templates/**`, `web/src/{registry.js,Builder.jsx}`, `server/settings.py`. **Then `npm run build` in `databricks-app/web`** — Databricks Apps does not build at deploy time, so an un-rebuilt `web/dist/` keeps serving fields the framework now rejects. |
| 6 | **Docs** | Every affected `docs/*.md`, then regenerate the derived trees with `python scripts/build_docs_reference.py` and `python -m mkdocs build`. Add new pages to `mkdocs.yml` nav. |
| 7 | **Agent skills** | `agent_skills/SKILL.md`, `reference/module_map.md`, `reference/onboarding_spec_full_reference.json`. Check `tool_specifications.json` / `dlt_observability_tools.json` too — they are usually loose object shapes with nothing to drift, but say so explicitly rather than assuming. |
| 8 | **Enhancement log** | A new `enhancement_logs/vX.Y.ZZ_enhancement_log.md`, following the existing format: scope table with IDs, previous-vs-current per enhancement, impacted assets, verification status, defects found, known gaps. |
| 9 | **Release notes** | A newest-first entry in `RELEASE_NOTES.md`. |

### Removals are rejected, never ignored

When an attribute is removed from the spec, `spec_validator.py` must **reject** it with a message
naming the replacement — not drop it silently. An ignored key still onboards, still writes its
control-table row, and still runs the pipeline, while quietly doing something other than what the
document says. For any attribute that switched a data-shaping behaviour ON, ignoring it flips that
behaviour OFF with no signal at all. Trigger on **presence, not truthiness**: `"flag": false` is
still a statement about a feature that no longer exists.

See `REMOVED_SOURCE_CONFIG_KEYS` / `REMOVED_TARGET_CONFIG_KEYS` /
`REMOVED_RECONCILIATION_FLOW_KEYS` / `REMOVED_CDC_LOAD_STRATEGIES` and `reject_removed_keys()` in
`spec_validator.py` for the established pattern.

### The Single-Read DAG mandate — four rules

- **1. Ingestion boundary: one external read per source table, per execution mode.** An external read
  (`spark.read`/`spark.readStream` against a storage path, Delta location or external catalog) must
  occur **exactly once per pipeline per required source table**. Five source tables means five base
  `@dlt.table` ingestion nodes. Never define two datasets that read the same raw path or external
  location. The one deliberate exception: identity is keyed **per execution mode** — a source consumed
  both as a stream and as a batch legitimately yields two nodes (`__stream` and `__batch`), because
  streaming and batch run on different primitives (checkpointed continuous state vs. point-in-time
  snapshot) and forcing one binding onto the other introduces checkpoint locking and full-refresh side
  effects. `bind()` raises rather than silently reading an MV as a stream.
- **2. Downstream lineage goes through `dlt.read()`.** Once a source is ingested into a base
  `@dlt.table`, every downstream transformation, enrichment, join, union and aggregation consumes it
  **exclusively** via `dlt.read("<name>")` / `dlt.read_stream("<name>")`. Let Lakeflow track lineage
  natively. A downstream node must never bypass the DAG to re-read the source path. In this framework
  that means: never add a direct read in a flow body — call `bind(plan, consumer_id, want_stream)`,
  which resolves to the base node. A fully-qualified three-part name is a *sibling reference*, not an
  escape hatch: `spark.read.table("cat.sch.tbl")` on a table this same pipeline publishes creates a
  real graph edge, exactly as `dlt.read` does.
- **3. "No intermediate tables" means no throwaway staging tables.** The prohibition targets persistent
  `@dlt.table` objects created *solely* to filter a status code, rename two columns, or stage a
  transient join before passing data along. Do that filtering, renaming and join prep directly inside
  the final consuming `@dlt.table`. Where a staging step genuinely aids readability and needs no
  independent analytical exposure, use `@dlt.view`. This does **not** apply to the rule-1 base
  ingestion nodes: those are the read-once boundary, and a `@dlt.view` cannot serve there, because a
  view is inlined into *each* consumer — "declared once" is not "read once", only materialization is.
  Shared base nodes stay pipeline-scoped `@dlt.table(temporary=True)` unless the spec explicitly asks
  for publication (the Intermediate Object Rule).
- **4. `materialize` policy.** The default is `"always"`: every external identity gets its own base
  node regardless of fanout. `"never"` is **hard-rejected** at onboarding validation — it would
  reintroduce silent redundant scans at fanout ≥ 2. `"auto"` is retained as a genuinely distinct
  *legacy* policy (materialize only at fanout ≥ 2, counted per `(identity, mode)`; a fanout-1
  identity stays `inline`), so a group onboarded before v1.7.3 keeps the topology it was onboarded
  with instead of silently gaining nodes on re-onboarding. It is a strictly weaker guarantee than
  `"always"` — do not use it for new specs. See `engine/source_plane.py` and the
  `reject_removed_keys()` pattern in `spec_validator.py`.
- **"Never put an eager action inside a dataset query definition" is wrong as stated.** The real
  prohibitions are eager-on-a-**streaming**-plan, self-read, and side-effecting writes. The batch
  branch of `dq/quarantine.py::_quarantine_table` runs `.agg(...).collect()[0]` inside a live
  `@dlt.table` closure and has shipped that way for releases. Any guard written to the blanket rule
  fails against the framework's own code. See `agent_skills/reference/common_pitfalls.md` §23–25.

### Two traps this repo has hit more than once

- **Never bulk-edit code with a broad regex.** A blanket `sed` once mangled Python identifiers
  during a column rename, and a non-greedy DOTALL `re.sub` once swallowed ~650 lines of
  `Builder.jsx`. Anchor on exact line prefixes and delete by index. Run `node --check` after
  touching the `web/src` tree.
- **Braces inside the DDL f-strings in `control_plane/ddl_definitions.py` must be escaped as
  `{{ }}`.** An unescaped `{...}` in a column COMMENT is evaluated as an expression and breaks
  every control-table DDL.

### Never deploy while a pipeline or test wave is running

`bundle deploy` prunes superseded artifacts from `<artifact_path>/.internal/` — on a UC Volume
exactly as in the workspace — so a deploy issued mid-update kills it with
`ENVIRONMENT_PIP_INSTALL_ERROR`. Unique per-deploy wheel filenames prevent overwrite-in-place, not
removal. See `flowx_testing/TESTING_PLAN.md` §0.
