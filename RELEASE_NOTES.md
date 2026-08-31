# Release Notes — NextGen Metadata Framework (Metaflow)

Semantic versioning: `MAJOR.MINOR.PATCH`. `pyproject.toml`'s `version` field is a build stamp
(UTC epoch-millis, rewritten by `scripts/bump_and_build.py` on every wheel build) and is
deliberately **not** the semantic version — that lives here and in `enhancement_logs/`.

---

## v1.5.0 — Reconciliation moves inside the pipeline DAG — 2026-08-31

Reconciliation used to be a job task that ran *after* the pipeline, reading whatever the pipeline
had most recently written. It is now a third first-class flow type that can run **inside** the
dataflow group's own Lakeflow update, beside ingestion and transformation — and underneath all
three there is now a source plane that makes every physical read happen exactly once.

Three things this release set out to make true, in plain terms:

- **One graph.** Ingestion, transformation and reconciliation all run inside one Lakeflow pipeline
  update per dataflow group.
- **Read once.** Every physical source table or path is read exactly once per update and reused by
  every consumer of it — including the three places the framework was demonstrably reading the same
  thing twice (a staged view read by both the clean and quarantine sides, nine transformation inputs
  over six distinct tables, and a reconciliation source re-read once per target).
- **Observability unchanged.** The observability engine stays a normal downstream Lakeflow job task.
  It was not moved into the graph, not wrapped, and not rewritten.

### Opting in

Nothing already deployed changes. The new `reconciliation_flows[].execution_mode` defaults to
`"job"` — today's standalone engine, byte for byte — and you move one flow at a time:

| `execution_mode` | What runs where |
|---|---|
| `"job"` (default) | the standalone `05_reconciliation_engine.py` task, exactly as before |
| `"pipeline"` | the comparison **and** the self-healing append, inside the pipeline update |
| `"pipeline_audit_only"` | the comparison inside the pipeline update; healing stays in job mode |

In the two pipeline modes the comparison stops being Python inside a job and becomes real Unity
Catalog tables — `recon__<reconciliation_id>__<target_id>__classified`, `__metrics` and
`__mismatch` — that you can query, that carry lineage, and that a new `dq_config` can attach
expectations to. `{"expr": "value_drift_count = 0", "action": "fail"}` on the one-row `__metrics`
dataset is the first declarative way a reconciliation threshold can fail an update. A new
`publish_schema` says where those three land; `source_plane` (top level) tunes the read-once
threshold, defaulting to `"auto"` so a single-consumer read keeps today's inline path and its
predicate pushdown.

### Three behaviour changes an operator must know before switching a flow to `"pipeline"`

1. **`error_handling.on_failure: "fail"` has a larger blast radius.** All targets still share one
   handler, so `"fail"` still stops the remaining targets of that `reconciliation_id`. What changes
   is what else it takes down: re-raising now fails the pipeline **update**, where before it failed
   one job task — so sibling reconciliation flows in the same group may already have run.
2. **The per-run log-silencing override moves to pipeline configuration, and applies to the NEXT
   update.** `pipelines start-update` accepts only `--full-refresh`, so the `recon_run_log_capture` /
   `recon_mismatch_log` widgets are replaced by the `dataflow.recon.run_log_capture` /
   `dataflow.recon.mismatch_log` pipeline configuration keys (same tri-state: absent or `""` defers
   to `logging_config`). Silencing a flow that is flooding the log tables is now a settings edit that
   takes effect on the next update, not the current one.
3. **Healing is source-change-triggered.** The heal lane fires per micro-batch of the reconciliation
   source, so an update in which that source does not advance performs no append. **Detection is
   unaffected** — the comparison datasets are recomputed every update, so drift and deletions are
   always found and the `dq_config` gate always evaluates; only the corrective write waits. If your
   recon source is static or low-change, keep `"job"`, or use `"pipeline_audit_only"`.

### Three silent bugs fixed on the way

- `two_tier_verification: false` used to onboard cleanly and then be **discarded** — the column was
  never written, so the runtime defaulted it back to `true`. Anyone who asked for the full
  comparison every run was silently getting the fingerprint short-circuit.
- `spark_config` had the identical defect. Both are now persisted, and a new regression test asserts
  that every attribute the validator accepts survives a round trip through the control table — the
  test whose absence let both rot.
- And then this release made the same mistake a third time, in the same function: `execution_mode`,
  `publish_schema` and `dq_config_json` were declared in the upsert schema and never written, so a
  spec asking for pipeline mode onboarded successfully, persisted NULL, read back as `"job"`, and was
  filtered out of the graph. **The feature could not turn on at all**, silently, until this was
  found. The round-trip test had not been extended to the new columns.

Also corrected: a long-circulating internal rule that said "never put an eager action inside a
dataset query definition". As stated it is false, and the framework's own shipped `quarantine.py`
falsifies it. The real prohibitions are eager actions on a *streaming* plan, self-reads, and
side-effecting writes.

### Verified live

This is not a design note. On **2026-08-31** a reconciliation flow in `execution_mode: "pipeline"`
ran end-to-end on `dev_metaflow`: job `metaflow_test_recon_dag_job` (id `854232399214818`) SUCCESS,
all four tasks green, pipeline `be78d88d-6064-414d-a10c-2aacd900fa86`. Its event log shows the
ingestion streaming table, the two L3 prepare datasets, the three L4 `classified` / `__metrics` /
`__mismatch` materialized views, the L5 `__pulse` streaming table, the heal `APPEND` flow and the
`foreachBatch` heal sink **all registered in one update** — one graph, exactly as advertised.

That run also settles an open question from the build: **`dlt.foreach_batch_sink` exists on DBR
serverless.** The heal lane is real, not theoretical. `"pipeline_audit_only"` was designed partly as
a fallback in case it did not; it remains a first-class setting for its own reasons (see the healing
note above and the `TRUNCATE_AND_LOAD` case below), not as a workaround.

Scenarios still **not** run live, and not claimed: audit-only mode, the read-once source plane across
three flow kinds, and the geneva `e41a47ba` topology — the last verified offline only. See
`metaflow_testing/TESTING_STATUS.md` §0b.

### Upgrading an existing workspace: run `setup_control_tables`

The four new control-table columns are nullable, but they are **not** free on a workspace that
already exists. Every control-table statement is `CREATE TABLE IF NOT EXISTS`, which is a no-op
against a table that is already there, so a column added to a `CREATE` reaches new installations
only. This release therefore ships an **additive migration** —
`ensure_control_table_columns()`, called at the end of `ensure_control_schema_exists`, strictly
additive, never a drop or a retype.

**`databricks bundle deploy` does not apply it.** Only *running* the `setup_control_tables` task
does. Deploy v1.5.0, go straight to onboarding a pipeline-mode flow, and you get `UNRESOLVED_COLUMN`
on `reconciliation_flow_spec` — which is exactly what happened on the test workspace, where that
table had none of `execution_mode`, `publish_schema` or `dq_config_json`. On a brand-new workspace
the migration is a no-op, since those columns are in the `CREATE` DDL too. Order on a fresh
workspace: `setup_control_tables` → onboard → run the pipeline.

### Backward compatibility

Nothing already deployed changes shape, and this was checked rather than asserted. `execution_mode`
NULL means `"job"`, resolved in Python (`getattr(row, "execution_mode", None) or "job"`) so a control
table that predates the migration still reads as job-mode instead of raising, and
`register_reconciliation_flow` independently early-returns on a job-mode row. On the workspace used
for the live run, **every pre-existing reconciliation row still reads NULL** after it, and no
job-mode flow registered a single node into any graph. Existing specs onboard unchanged; existing
job resources keep running the standalone engine.

One correction landed here late: the new graph-cycle validations were initially hard errors in job
mode too, which stopped a shipped job-mode spec from onboarding at all. Cycle findings are now errors
in pipeline mode and warnings in job mode — a Lakeflow graph cycle is not a thing that can exist when
the engine runs after the update has finished.

### A flow whose source is a `TRUNCATE_AND_LOAD` target must use `pipeline_audit_only`

A `TRUNCATE_AND_LOAD` target is rewritten in full on every update, and Delta refuses to stream from
it. Such a flow is now rejected at plan time under `"pipeline"`, with an error naming
`"pipeline_audit_only"` as the setting to use — previously it passed every check and then failed
mid-update with `DELTA_SOURCE_TABLE_IGNORE_CHANGES`.

### New convention: onboarding is delegated

New jobs no longer inline a `02_onboarding_engine.py` notebook task. They call the generic
parameterised `resources/onboarding_job.yml` via `run_job_task`, passing `spec_file_path`, `catalog`,
`env` and `action_type` as job parameters. Its sibling
`resources/framework_config_onboarding_job.yml` onboards a whole directory instead of one spec. The
~20 pre-existing legacy `metaflow_test_*_job.yml` files keep their inline copies deliberately — new
orchestration is added alongside legacy jobs, not retrofitted into them.

Full detail, including the seven defects fixed between the initial build and the passing live run:
`enhancement_logs/v1.5.00_enhancement_log.md`.

---

## Spec Builder ↔ framework agreement: ten corrections — 2026-08-30

v1.4.0 propagated a breaking spec change into the Databricks App by working through a hand-written
checklist. This release checks the result the only way that proves anything: by driving the app's
own output through `onboarding/spec_validator.py` and comparing. Ten disagreements surfaced between
what the Spec Builder offers or ships and what onboarding actually accepts.

**One of them was written into the checklist itself.** The attribute delta's entry for
`cdc_operation_column` stated the correct applicability in its `after` field —
`{SCD1, SCD2, FULL_SNAPSHOT_CDC}` — and then told the app to *"simplify the predicate to
`isCdc(strategy)`"*, which is `{SCD1, SCD2, SCD3, FULL_SNAPSHOT_CDC}`. The app implemented the
instruction rather than the fact, so the builder offered a delete-marker field on SCD3 and
onboarding rejected every spec that used it. Reading a checklist back to itself cannot find that;
running its output through the validator finds it immediately.

### What was wrong

**Builder offered fields onboarding rejects.** `cdc_operation_column` and
`cdc_operation_mapping.delete_values` on SCD3 — a current/previous pivot with no delete path at
all. Fixed with a `hasDeleteMarker` helper in place of `isCdc`, in both the frontend and the
server-side registry.

**A malformed predicate silently hid two fields entirely.** Removing the
`FULL_SNAPSHOT_CDC_NO_PK` guard deleted the operand and left the operator behind:
`{"ne": ["target_config.cdc_load_strategy"]}`. `evaluate_predicate` reaches its binary operators
behind a `len(args) >= 2` check and otherwise returns `False`, so that predicate was false for
every input and the delete-marker fields were invisible for every strategy — no error, no warning.
A new test now rejects any binary operator with fewer than two operands anywhere in the registry.

**The app's validation rules had drifted from the framework's.** `primary_keys` was not required
for `FULL_SNAPSHOT_CDC` (it has been required by every CDC strategy since v1.4.0), and
`columns_to_exclude` was rejected on two of the three strategies that reject it. Both corrected;
the second is now stated as the complement of the comparison-capable strategies so a future
strategy cannot fall through it. Two rules added: `cdc_operation_column_scope` and
`kafka_sink_requires_options`.

**Kafka sinks could not be authored at all.** `sink_config.format` offered `"kafka"`, but nothing
in the app could set `kafka_options` — which the framework requires, and which must carry both
`kafka.bootstrap.servers` and `topic`. Added as a `kv` field. Fixing it exposed a second bug: flow
validation-rule contexts exposed only flat scalars, so *any* rule referencing a `kv` or `repeat`
path resolved to nothing whether it was filled in or not. Three more framework-supported attributes
were added while there: `export_file_name_format`, `sign_passphrase_secret`, and
`pre_extraction_decryption.passphrase_secret` — the PGP private key's passphrase, which sits beside
`secret_passphrase`, the ZIP archive's password, and differs from it by word order alone. Both now
say which is which.

**Two templates shipped specs that could not onboard.** `reconciliation/blank.json` carried the
builder's decomposed target keys instead of the canonical three-part `table`; and the framework's
own `pipeline_onboarding_template.{json,yaml}` pointed `transform_sql` at `missing_records`, a view
nothing creates — the appender registers the miss set as `_reconciliation_unmatched_records`. The
app's copy of that template was already correct, so the two copies had silently forked.

**The agent skill overstated CDC field applicability**, describing `columns_to_exclude` and
`cdc_operation_column` as available on every CDC-dispatched strategy when both are narrower. It now
carries a per-field valid-for/rejected-on table naming the enforcing sets in `spec_validator.py`,
plus a new section on `encrypted_columns[].source_data_type` — the one attribute v1.4.0 *added*,
which the skill had not mentioned.

### What stops it recurring

`databricks-app/tests/test_framework_spec_agreement.py` imports the real `validate_spec` and, on
every run, drives every shipped template and every (strategy × attribute) pair through it. Each of
its assertions was re-run against the pre-fix input first, to prove it fails rather than passing
vacuously.

The wrong instruction in `docs/v1.4.0_json_attribute_delta.json` is corrected in place, with the
original preserved beside it under `app_action_superseded` so the contradiction stays visible. All
ten corrections are recorded there under `app_surface_corrections`, machine-readably, for any agent
replaying that delta.

**No framework source changed** — no validator, no schema, no DDL. Every defect was a *description*
of the framework disagreeing with the framework. `pytest tests/unit` is byte-identical to the
v1.4.0 baseline (502 passed, 8 pre-existing failures, 113 Spark-fixture errors); the app suite went
from 62 to 113 passing. `web/dist/` was rebuilt.

### Deployed and verified on `dev_metaflow` — 2026-08-30

After the corrections above: pre-flight (0 active runs, all 49 pipelines terminal) → `bundle
validate` OK → `bundle deploy` (**1164 files, 88 resources, 0 failed**, wheel
`0.0.1788077685776` built and published by the deploy itself) → app deployed → control-table
setup → **`framework_config_onboarding_job`: 43/43 specs onboarded, 0 failures.**

That closes the bulk-onboarding regression check v1.4.0 listed as blocking a live-verified claim.
It is the strongest live evidence yet for the v1.4.0 removals — every spec in the corpus went
through the real `validate_spec` on serverless compute, and a single surviving removed attribute
anywhere would have failed its spec by name. **No Lakeflow pipeline was run**, so the
pipeline-level gap (TC-CDC-007 in particular) is narrowed, not closed.

**One trap found and worth knowing: `bundle deploy` does not deploy the app.** It syncs the
source and reports success, but creates no app deployment — after a clean deploy, the newest
deployment was still the previous day's, with no warning anywhere, and the running app kept
serving the pre-correction bundle. `databricks bundle run metaflow_onboarding_app` is a required
second step; the check that proves it landed is comparing the deployed asset hash against the
local `web/dist`. This compounds the existing "rebuild `web/dist`" rule: rebuilding is necessary
but not sufficient.

Control-table structure is unchanged by design — `recon_mode` and `generate_surrogate_key` remain
physically present on `reconciliation_flow_spec`, since `01_setup` only issues
`CREATE TABLE IF NOT EXISTS`. No `ALTER TABLE … DROP COLUMN` was issued.

### One thing left for a human to decide

`databricks-app/web/dist/` is not tracked by git and never has been. `.gitignore` says it should
be — a global `dist/` followed by `!databricks-app/web/dist/**` — but git never descends into an
excluded directory, so a negation for files beneath one can never match. `git ls-files` on that
path returns nothing, while the v1.4.0 log and a test docstring both describe it as committed
output. On a fresh clone there is no bundle, and DABs `sync` honours `.gitignore`, so a deploy from
a clean checkout would ship an app with no frontend. The `.gitignore` is corrected here (unignoring
the directory before its contents, verified with `git check-ignore`), but **the bundle has not been
added** — committing ~315 KB of generated output per frontend change is a repo-policy call, and
Databricks Apps does not build at deploy time, so the alternative is a documented pre-deploy build
step. Either is defensible; a negation that silently does nothing is not.

Full detail: [`enhancement_logs/v1.4.01_enhancement_log.md`](enhancement_logs/v1.4.01_enhancement_log.md).

---

## Load-strategy guidance: when to use each, and what it risks — 2026-08-30

The six `cdc_load_strategy` values were documented by *behaviour* — what each one does — but never
by *judgement*: which to pick, and what each one costs when it is the wrong pick. Both silent
data-loss paths in the framework live in this choice, and neither was stated anywhere an author
configuring a spec would see it.

### Changes

**1. Curated attribute prose (`databricks-app/config/attribute_knowledge.curated.json`).** The
hand-written layer grew from 17 to 23 entries. `target_config.cdc_load_strategy` now carries a
per-strategy use-when tip and seven symptom/cause/fix entries; six CDC attributes that previously
had only registry-derived text got real prose: `sequence_by_column`, `columns_to_check`,
`columns_to_exclude`, `cdc_operation_column`, `empty_target_if_source_empty`,
`generate_hash_columns`. This layer feeds both the app's attribute inspector and the generated
JSON reference, so the two cannot drift.

The two failure modes now stated explicitly wherever the attribute appears:

* **`TRUNCATE_AND_LOAD` blanks a populated target** when its source returns zero rows. The
  `empty_target_if_source_empty` guard has had no runtime effect since it was withdrawn on
  2026-08-29.
* **`FULL_SNAPSHOT_CDC` pointed at an incremental feed deletes every key not in the current
  batch.** Nothing validates that the source is actually complete.

Also newly documented: the fallback sequencer (`__framework_ingestion_timestamp_utc`) is
`current_timestamp()` evaluated once per batch, so two versions of a key inside one batch tie
non-deterministically — and on SCD2 a tie corrupts history order, not just the surviving value.

**2. `docs/03_transformation_and_cdc.md` gains §2.0 "Choosing one".** A three-question decision
table (keyed on the *source*, not the target), a use-when / avoid-when / worst-failure-mode table
for all six strategies, and the shared-behaviour paragraph for the four merging strategies. The
section intro said "7 built-in" strategies; there are 6 since `FULL_SNAPSHOT_CDC_NO_PK` was
removed in v1.4.0.

**3. `FULL_SNAPSHOT_CDC` was unselectable in the server-driven form.**
`config/registry/shared.cdc.json` carried five strategy tabs, not six — the strategy existed in
`web/src/registry.js` but had never been added to the server registry copy, and
`target_config.primary_keys` was hidden for it despite being required. Both fixed.

**4. Stale help text corrected.** `empty_target_if_source_empty` described itself as working
("false, the default and the safe choice, leaves the target untouched") in both `Builder.jsx` and
the server registry. It now says it is withdrawn, why an in-graph guard cannot work, and where to
enforce the policy instead.

**5. Generated references regenerated.** `docs/reference/json/` was stale against the knowledge
base: it still documented `generate_surrogate_key` and `recon_mode`, and its CDC strategy table
still listed `FULL_SNAPSHOT_CDC_NO_PK` — all three removed in v1.4.0. Regenerating dropped them
and added the missing `other.md` page (already present in `mkdocs.yml` nav but never generated);
`scripts/build_docs_reference.py` gained the title and intro that page needed. The embedded app
wiki was rebuilt to match — 124 pages, 166 attribute deep links.

### Note for the next regeneration

`attribute_knowledge.json` is normally produced end to end by
`databricks-app/scripts/build_attribute_knowledge.py`, whose first stage shells out to `node` to
dump `registry.js`. Node is not installed on this machine, so the curated overlay was applied
directly, replicating that script's merge exactly (curated wins field-by-field). Re-running the
real generator where node is available should be a no-op for these entries — worth confirming once.

---

## Spec Builder app — embedded docs become the full framework wiki — 2026-08-30

The app's `/docs` route served a single hand-written page covering only the attribute reference.
It now serves the complete MkDocs wiki — **124 pages**: architecture, per-subsystem functional
docs, onboarding, both generated references (JSON attributes and code), the FAQs, known
limitations, the architecture review, and the archive — with Material's tabbed navigation and
full-text search over all of it.

### Why the old page had to go rather than be extended

It duplicated `docs/` by hand, so it could only ever drift, and it violated the app's own
zero-hardcoding principle: 21 section headings and their anchors were literal HTML. Extending it
to cover the whole framework would have meant hand-maintaining a second copy of every document.
The wiki was already being built from `docs/` by `mkdocs.yml` — the app simply was not serving it.

### Changes

**1. `/docs` serves the built wiki.** `scripts/build_app_docs.py` (new) runs `mkdocs build` and
syncs the output into `databricks-app/docs_site/`. It has to live inside `databricks-app/` because
Databricks Apps upload only `source_code_path` — the same reason `web/dist/` is a committed build
artifact. `server/app.py` needed no change; it already mounted `/docs` on that directory.

**2. Previously excluded material is now navigable.** `archive/` (68 pages) and
`architecture_review/` (9 pages) were in `exclude_docs`, which made the wiki an incomplete account
of the framework. Both now build under a **Project record** tab. The archive is reached through a
generated landing page (`docs/archive/index.md`) grouped into superseded guides and the test-case
catalogue, each carrying a banner stating the content is unmaintained — available for provenance,
impossible to mistake for current behaviour.

**3. Attribute deep links now target an attribute's own heading.** Previously every
`#anchor` pointed into the one hand-written page. `config/docs_index.json` (generated) maps all
**166** attribute paths to `reference/json/<flow>/#<attribute>`, using the same slug rule and the
same `attribute_knowledge.json` source that renders those pages — so a link cannot outlive the
heading it targets. `/api/docs/resolve` prefers it; `docs.json`'s hand-maintained `anchors` map
remains the fallback. The index also reaches the frontend via `/api/config`, so the attribute
inspector links without a round trip.

**4. Three dead anchors in `docs.json` fixed.** `#4-target-config--cdc-reference`,
`#6-governance--tagging` and `#11-template-variables--parameter-substitution` used a double dash
where MkDocs renders one, so those five entries silently landed readers at the top of the page.

**5. A back-link to the builder.** A root-relative `Spec Builder app` nav tab returns the reader
to the app from any depth in the wiki.

### Fixed alongside

**Duplicate class members in `web/src/Builder.jsx`.** `componentDidMount` and `docsBase` were each
defined twice; JS silently keeps the later definition, so the earlier pair was dead code and the
Vite build warned on every run. The dead `docsBase` also hardcoded `http://localhost:8000/`, which
would have been wrong in a deployed app had it ever been the live one.

### Verification

- `pytest databricks-app/tests/` — **62 passed**. Three tests asserted the old single-page
  contract (its title, its 21 anchors, resolution into the master index) and now assert the wiki
  contract instead: that each audience section is reachable, that Material's nav renders, and that
  a resolved deep link's anchor actually exists on the page it names.
- New `test_every_indexed_attribute_deep_link_resolves` walks all 166 generated links and fails on
  any anchor the wiki does not have — the guard that `docs_index.json` was regenerated after a
  heading change.
- Checked live against the running app: 166 indexed attributes + 16 `docs.json` anchors + 21
  `docs.json` pages — **0 dead links**.
- `scripts/build_app_docs.py --check` fails on stale committed output, for CI.

*Impact:* run `python scripts/build_app_docs.py` after editing anything under `docs/`; the built
wiki and `docs_index.json` are committed artifacts. `mkdocs.yml` no longer excludes `archive/` or
`architecture_review/`, and link-anchor validation is now on (`validation.links.anchors: warn`),
which surfaces pre-existing broken anchors in the doc sources — those are reported, not yet fixed.

---

## v1.4.0 — Attribute deprecations, Databricks-native snapshot CDC, observability parameter contract — 2026-08-30

Five spec attributes and one CDC strategy removed, one added; the reconciliation engine narrowed
to a single execution model; the triggered observability engine given an explicit, validated
four-parameter contract; and the bundle's artifact path reverted to the DABs standard.

Full engineering record — per-enhancement previous-vs-current state, impacted assets, verification
status and defects found during implementation — in
[`enhancement_logs/v1.4.00_enhancement_log.md`](enhancement_logs/v1.4.00_enhancement_log.md).
Attribute-level delta for automated app updates:
[`docs/v1.4.0_json_attribute_delta.json`](docs/v1.4.0_json_attribute_delta.json).

**Every removed attribute is REJECTED at onboarding, never ignored.** That is the load-bearing
decision in this release. An ignored key lets the spec onboard, writes the control-table row and
runs the pipeline — while quietly doing something other than what the document says. For
`normalize_column_names` and `generate_surrogate_key` specifically, ignoring would flip a
data-shaping behaviour from ON to OFF with no signal at all.

### ⚠️ Breaking Changes

**1. Artifact packaging reverts to the standard workspace path.** `workspace.artifact_path` is
removed from the `dev_metaflow` target; both targets now use the DABs standard
`${workspace.root_path}/artifacts`. The path it replaced was
`/Volumes/metaflow/framework/wheels/${workspace.current_user.short_name}` — a *dynamic*,
per-target, per-user UC Volume path.

Three reasons, in order of how often they bit:

1. **Deployment conflicts.** A per-user path forks the artifact location by whoever ran the
   deploy, so two engineers deploying the same target published to two different places and each
   rewrote `../dist/*.whl` to their own. One location per target is what makes a deploy
   reproducible.
2. **Volume provisioning is not free.** The `dev` target cannot have a Volume artifact_path at
   all — its metastore is at its volume ceiling (52 estimated vs. a limit of 50) and its
   `metaflow` catalog is at 51 schemas. Two targets diverging on where artifacts live is exactly
   the drift the `artifacts` block exists to prevent.
3. **It did not buy what it was adopted for.** The hoped-for benefit was that a UC Volume never
   prunes, so an in-flight Lakeflow update could keep installing an older wheel across a redeploy.
   Verified false on 2026-08-30: DABs prunes superseded artifacts from `<artifact_path>/.internal/`
   on a UC Volume exactly as it does in the workspace (`.internal/` went from two wheels to one
   across a deploy).

*Impact:* the operational rule is now the only mitigation for the in-flight-update hazard, and it
is unchanged — **never `bundle deploy` while a test wave or pipeline is running**
(`metaflow_testing/TESTING_PLAN.md` §0). Wheels published by the older manual
`scripts/build_and_upload_wheel.py` still sit in the Volume root outside `.internal/`; DABs never
managed or pruned those, so any pipeline still pinned to one keeps working.

**2. `source_config.normalize_column_names` removed.** `source_config.column_normalization`
`{enabled, case}` is the only switch; `enabled` defaults to `false`.

*Why:* it was a second way to say what `column_normalization.enabled` already said. Carrying both
meant `ingestion/column_normalization.py` owned a three-level precedence ladder, a
present-vs-truthy distinction on `enabled`, and a contradiction warning — roughly forty lines whose
only job was deciding which of two synonyms won.

*Impact, and one subtle case:* before v1.4.0, a `column_normalization` object that omitted
`enabled` deferred enablement to the legacy boolean, so `{"case": "preserve"}` on its own was
**on**. It now reads as written: **off**. A spec relying on that deferral must add the explicit
`"enabled": true`.

| Before | After |
|---|---|
| `{"normalize_column_names": true}` | `{"column_normalization": {"enabled": true}}` |
| `{"normalize_column_names": false}` | delete the key |
| `{"normalize_column_names": true, "column_normalization": {"case": "preserve"}}` | `{"column_normalization": {"enabled": true, "case": "preserve"}}` |
| `{"column_normalization": {"enabled": true, ...}}` | **no change** |

**3. The surrogate-key engine is removed in full.** Deleted: the module
`src/.../crypto/hashing.py`, the `__framework_surrogate_key` column, and four spec attributes —
`target_config.generate_surrogate_key`, `target_config.surrogate_key_columns`,
`target_config.surrogate_key_exclude_columns`, `reconciliation_flows[].generate_surrogate_key`.

**4. `cdc_load_strategy: "FULL_SNAPSHOT_CDC_NO_PK"` is removed.** Full-snapshot ingestion now
relies strictly on the Databricks-native CDC pattern —
[`apply_changes_from_snapshot`](https://docs.databricks.com/aws/en/ldp/cdc) over a real
`target_config.primary_keys`.

*Why (3 and 4 together):* `apply_changes_from_snapshot` requires `keys`. With no natural key the
framework manufactured one — a SHA-256 over *every payload column of every row*, recomputed on
every run and forced on even against an explicit `generate_surrogate_key: false`. It cost a
full-width hash per row per snapshot; it made row identity depend on the exclusion list staying
correct (a single volatile column leaking into the basis re-keyed the entire table, and
`apply_changes_from_snapshot` then read that as a delete-and-reinsert of everything — a defect this
framework actually shipped and had to fix); and it did not model the data, since two rows identical
in every column were one row to the hash and a Day-2 field change was reported as a delete plus an
insert rather than the update it was.

*Migration:*

| Situation | Replacement |
|---|---|
| The source has a natural key, it was just never declared (the common case) | `FULL_SNAPSHOT_CDC` + `primary_keys`. An update is now reported as an update. |
| The source genuinely has no key | `TRUNCATE_AND_LOAD` — an honest full refresh instead of a synthetic diff. |

> Verify the candidate key is actually unique in the snapshot before committing to it
> (`SELECT k, count(*) FROM src GROUP BY k HAVING count(*) > 1`). A non-unique key produces a
> target that looks fine and silently collapses rows.

Legacy columns are left alone: a table materialized before the upgrade still physically carries
`__framework_surrogate_key`. It is not dropped, `cdc/comparison_columns.py` still excludes it from
comparison resolution, and `storage/column_ordering.py` no longer front-loads it.

**5. `reconciliation_flows[].recon_mode` removed — reconciliation is triggered-only.** Both former
values are rejected, `"triggered"` included: it is removed as an *attribute*, so a spec asserting
the surviving behaviour still names a field that does not exist. Also removed: the `recon_mode`
widget on `05_reconciliation_engine.py`, and the `continuous` / `processing_time` parameters on
`reconciliation/streaming.py::run_streaming_target_reconciliation`.

*Why:* `"continuous"` wrapped a standing stream around a **batch-shaped** unit of work.
`run_target_reconciliation` writes one `reconciliation_run_log` row per invocation and checks a
batch fingerprint for idempotency, so a never-ending query produced a log row per micro-batch whose
fingerprint could never repeat, and counts describing an arbitrary slice of wall clock rather than
a comparison anyone asked for. It also never ran where this framework runs — a standing trigger on
serverless job compute raises `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`.

*Impact:* every run is now bounded — batch reads, and `trigger(availableNow=True)` for a
`read_mode: "streaming"` side, draining the backlog and stopping. **To reconcile more often,
schedule the job more often.** One consequence is a simplification: `task_run_id` narrowing of a
side declaring `task_run_id_column` is now unconditional, where it used to be gated on the mode.

**6. Python API signature changes** (breaking for any direct caller):

| Function | Change |
|---|---|
| `reconciliation/matcher.py::prepare_dataset_for_matching` | trailing `generate_surrogate_key` parameter removed |
| `reconciliation/appender.py::run_target_reconciliation` | positional `generate_surrogate_key` removed (it sat between `compare_columns` and `source_hash_precomputed`) |
| `reconciliation/streaming.py::run_streaming_target_reconciliation` | `generate_surrogate_key`, `continuous`, `processing_time` removed |
| `dq/quarantine.py::_apply_hash_and_surrogate_key_columns` | renamed `_apply_hash_columns` |
| `crypto/hashing.py` | module deleted (`generate_surrogate_key_hash`, `resolve_surrogate_key_columns`, `SURROGATE_KEY_COLUMN`, `DEFAULT_HASH_EXCLUDED_COLUMNS`) |

**7. Observability: `run_pipeline_update_run_id` renamed to `pipeline_task_run_id`, and three more
parameters are now required.** The old name baked one convention — a task literally called
`run_pipeline_update` — into the parameter name, so it read as a lie in every job whose pipeline
task is called something else.

The triggered engine now requires **four** task parameters: `dataflow_group_id`, `catalog`, `env`,
`pipeline_task_run_id`. Two of these changed status:

- **`dataflow_group_id` was derived; it is now declared**, with the derived value kept as a
  *cross-check*. A derived value cannot detect the most likely wiring mistake there is —
  `depends_on` pointing at the wrong `pipeline_task`, or a copy-pasted observability block still
  pointing at the job it came from — because whatever pipeline the task lands on reports *its*
  group id happily, and the export succeeds while describing the wrong dataflow. A disagreement now
  raises and names both. A pipeline that declares no `dataflow.group.id` at all is **not** an
  error; the cross-check logs at INFO and the declared value stands.
- **`env` replaces the optional `deployment_environment` widget.** Optional environment labelling is
  worse than none: telemetry that omits it is silently merged with every other environment's in the
  consumer, and nobody notices until a prod alert fires on dev data.

*Impact:* every job wiring an `observability_export` task must add `dataflow_group_id`, `env`, and
rename the run-id parameter. Both in-repo jobs (`resources/dlt_observability_job.yml`,
`resources/metaflow_test_obs_003_vol_export_job.yml`) are updated.

### ✨ Added

| # | Change |
|---|---|
| 1 | **`target_config.encrypted_columns[].source_data_type`** — the declared original Spark type of a column being encrypted (`"string"`, `"decimal(18,2)"`, `"timestamp"`, …). Encryption replaces a column's physical type with ciphertext binary, so the pre-encryption type is recorded as the Unity Catalog `original_data_type` tag, which is what a downstream `decrypted_columns[].cast_to_type` is validated against. **Optional and safely defaulted:** omitting it uses the type Spark reports at encryption time — the pre-v1.4.0 behaviour — so no existing spec needs editing. Declaring it turns a silent source type change into a loud `CryptoError` at encryption time (declared vs. observed compared case-insensitively) instead of silently re-tagging and breaking the decrypt side later. |
| 2 | **`observability/runtime_params.py`** — the triggered engine's whole parameter contract, validated in one call before a `WorkspaceClient` is constructed and before any API call, table read or dispatch. Reports **every** missing parameter at once rather than one per redeploy, and catches the failure mode a mistyped task key actually produces: the Jobs service substitutes nothing and passes the literal `{{tasks.<typo>.run_id}}` text through, which without this check surfaces later as a confusing "must be numeric" complaint about a value nobody typed. |
| 3 | **Key-presence guard on snapshot CDC.** A `primary_keys` entry that never reaches the clean upstream (renamed by `column_normalization`, projected away by `data_standardization_sql`) is now caught inside the snapshot-input dataset — where `source_view` first has a schema — and reported with the available columns and the two usual causes, instead of surfacing as a generic missing-key error from `apply_changes_from_snapshot`. |

### 🏛️ Design decisions recorded

**`pipeline_task_run_id` is a task parameter and must NOT be declared in `pipeline_parameters`.**
`{{tasks.<key>.run_id}}` is a Jobs *dynamic value reference*, resolved by the Jobs service per
**job run** at the moment the downstream task is dispatched. A pipeline's `configuration:` block is
resolved by the Pipelines service per **pipeline update** and is static for that deployment — there
is no job run in scope for a task value to resolve against, so declaring it there cannot work and
would at best pin every run to a stale literal. The general rule: *dynamic, per-run values travel as
task parameters; static, per-deployment values travel as pipeline configuration.* Nothing about a
pipeline resource changes to support the observability task. Written up in
[`docs/08_observability_and_telemetry.md`](docs/08_observability_and_telemetry.md) §1.1.1.

**A continuous destination's event-log tables belong in the observability block, not in pipeline
settings.** `destination_config.event_log_tables` on the `mode: "continuous"` row is canonical:
(1) a continuous export fans one streaming read out to N destinations, and two destinations
covering different subsets of the estate is a normal shape that a single pipeline-level list cannot
express; (2) adding a newly-onboarded pipeline's event log must be a control-table upsert, not a
bundle deploy — and this repo's own rule is never to deploy while a pipeline is running, which is
the state a continuous export is always in; (3) the mode, destination type, credentials, retry
policy and source tables are all attributes of the same destination. The pipeline-level
`dataflow.otel_streaming.event_log_tables` remains as an explicitly subordinate **bootstrap
fallback**. `dataflow.group.id` and `dataflow.control.catalog` correctly stay on the pipeline —
they are what let it find its own rows. Written up in
[`docs/08_observability_and_telemetry.md`](docs/08_observability_and_telemetry.md) §6.1.

**`hash_precomputed` is an assertion, not an instruction.** It does not precompute row hashes over
`match_keys`, and it does not expect a third party's own hash column. `true` declares that *this
framework* already wrote `__framework_hash_key`/`__framework_hash_value` onto the table upstream, at
CDC-materialization time, and they are trusted verbatim — `match_keys`/`compare_columns` are not
hashed at all. `false` (the default) computes both **now**: `__framework_hash_key` over `match_keys`
in the declared order, `__framework_hash_value` over the resolved `compare_columns` alphabetically
sorted. A `true` on a dataset lacking the columns is a hard `FrameworkConfigError`, not a silent
recompute. The precondition that makes the trust safe — and which fails **silently** when violated —
is that the upstream flow's `primary_keys` are this flow's `match_keys` and its comparison columns
are this flow's `compare_columns`; when they disagree nothing errors, the hashes simply never match
and every row reports as drifted. Full mechanics and a best-practice table in
[`docs/07_reconciliation_engine.md`](docs/07_reconciliation_engine.md) §9.

### 🧪 Test scenario rewritten

**TC-CDC-007** was "Full Snapshot Diffing *Without PK*" — the only scenario whose entire subject was
the removed strategy. Its fixture (`sample_mainframe_customer_master_day1/day2.csv`:
`customer_name`, `customer_city`, `customer_status`) has `customer_name` unique across all 10 rows,
so it is now the declared key. That makes the Day-2 assertion **stronger**: a changed
`customer_status` is now verified as an `UPDATE` in place, where the payload hash reported it as a
delete plus an insert — the same entity under two identities. Resource filenames keep their `_nopk`
suffix so existing bundle references and run history stay valid.

### ✅ Verification

- `pytest tests/unit` — **502 passed**. The 8 pre-existing failures
  (`test_config_validation_negative_spec.py`, `test_optional_fields_df_customer_ingest_spec.py`) are
  unrelated: they read `test_specs/*.json`, a directory that is not in the repo. The 113 errors are
  the Databricks-Connect `spark` fixture requiring workspace auth, unavailable locally.
- **34 new unit tests**, both pure-Python (no Spark):
  `tests/unit/test_removed_attributes_v140.py` (18) asserts each removed attribute is *rejected*
  with a migration message rather than ignored, that presence rather than truthiness is the trigger,
  and that `source_data_type` is accepted, optional and type-checked;
  `tests/unit/test_observability_runtime_params.py` (16) covers the four-parameter contract, the
  report-everything-at-once behaviour, the unresolved-`{{tasks…}}` literal, and the
  `dataflow_group_id` cross-check including the not-an-error `None` case.
- `pytest databricks-app/tests` — **61 passed** (was 60; one added). The new test asserts the removed
  attributes are absent from the **built** `web/dist` bundle, not just from `registry.js` — closing
  the gap where an un-rebuilt frontend would keep offering fields the framework now rejects.
- `npm run build` in `databricks-app/web` re-run; `dist/assets/` regenerated and verified to contain
  zero occurrences of the removed attribute names.
- `python scripts/build_docs_reference.py` re-run — `docs/reference/json/` and `docs/reference/code/`
  regenerated from `registry.js` and the source AST.

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
