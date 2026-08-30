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
| 3 | **JSON schema + templates** | `onboarding_templates/onboarding_spec.schema.json`, plus `pipeline_onboarding_template.{json,yaml}` (these two are asserted equivalent by `test_spec_loader.py` — change both), `onboarding_spec_full_reference.{json,md}`, and any affected `metaflow_testing/*.json`. |
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
removal. See `metaflow_testing/TESTING_PLAN.md` §0.
