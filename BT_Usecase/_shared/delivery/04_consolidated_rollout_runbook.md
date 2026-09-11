# 04 — Consolidated Rollout Runbook

**FlowX (NextGen Metadata Framework) · UC3, UC6, UC7**
**Framework release:** v1.7.4 · **Wheel artefact version:** 0.0.3
**Document date:** 5 September 2026
**Status:** For review and sign-off prior to rollout
**Audience:** The engineer performing the rollout, and whoever signs it off

---

## What this document is

The ordered sequence that takes the estate from nothing to three running, governed use cases with a
Beta-marked application in front of them. Ten steps, each naming which artefact it uses, what "done"
looks like, and the traps that apply.

It also carries the cross-alignment verification record — the checks made between the setup notebook
and the asset register, and how each discrepancy was resolved — and the open assumptions that need a
human decision.

Follow this document; refer to the others as each step directs.

## Readiness position before you start

| Dimension | Position |
|---|---|
| **Use-case pipelines** | Built and exercised. UC7 has a full test report; UC3 and UC6 have onboarding specs and bundle resources in place. |
| **Environment provisioning** | **Manual today.** The setup notebook in document 02 is new and unrun. Every landing volume is an undeclared manual prerequisite. |
| **Governance** | **Partial and uneven.** UC3 and UC6 apply tags; UC7 applies none at all. No column masks or row filters exist anywhere in the estate. |
| **Catalog migration** | Outstanding. The Metaflow-to-FlowX catalog rename is still a manual, pending operator action. |

The honest summary: **the pipelines are ready; the estate around them is not.** This runbook is the
difference between three use cases that run and three use cases that can be handed to an operations
team.

## A note on the catalog name

Throughout this delivery set the Unity Catalog name is **always a parameter, never a literal**.
Every example uses **`br_digital_poc`**.

Wherever a path is written as `/Volumes/<catalog>/...`, substitute the catalog for the deployment.
In the setup notebook this is the `target_catalog` widget; in a bundle it is `${var.catalog}`; in an
onboarding spec it is the `{{catalog}}` token. So the UC7 landing path resolves as:

```text
/Volumes/br_digital_poc/landing/uc_7/raw/EMSC/
```

**One caveat that is not cosmetic.** `resources/uc7/*.yml` currently hardcodes its catalog rather
than using `${var.catalog}`. Until that is parameterised, UC7 cannot be pointed at another catalog
by changing a variable alone, and it is a prerequisite for any catalog migration.

---

## The delivery set

| # | Document | Covers |
|---|---|---|
| 01 | [Use case asset inventory and governance](01_usecase_asset_inventory_and_governance.md) | Asset register by layer, DAG lineage, dashboards, tagging, sensitivity, access control |
| 02 | [Environment deployment and setup](02_environment_deployment_and_setup.md) | The setup and staging notebook, its design, and how to run it |
| 03 | [Databricks naming and best practice standards](03_platform_best_practice_and_naming_standards.md) | Naming templates for every Databricks object, tag vocabulary, build practice |
| 04 | [Consolidated rollout runbook](04_consolidated_rollout_runbook.md) | The ordered end-to-end sequence, cross-alignment record, open assumptions |

Each document stands alone. Where one depends on another it links rather than repeats.

---

## The runbook

This part is a synthesis, not a restatement. It is one ordered sequence that an engineer follows end
to end, drawing on the artefacts in Parts A, B and C at the points where each is needed. Each step
states which artefact it uses, what "done" looks like, and the traps specific to that step.

### Before you start — four traps that apply throughout

These recur across steps and are stated once, here, rather than repeated at each.

| Trap | Why it bites |
|---|---|
| **Never deploy while a pipeline or test wave is running.** | `bundle deploy` prunes superseded artefacts from `<artifact_path>/.internal/`, on a Unity Catalog volume exactly as in the workspace. A deploy issued mid-update kills the running update with `ENVIRONMENT_PIP_INSTALL_ERROR`. Unique per-deploy wheel filenames prevent overwrite-in-place, **not** removal. |
| **`bundle deploy` does not update the application.** | `bundle run` is required. A deploy reports success while the app continues serving its previous code. |
| **`npm run build` before deploying any application UI change.** | Databricks Apps does not build at deploy time. An un-rebuilt `web/dist/` serves the old bundle, so a new banner simply will not appear. |
| **`setup_control_tables` runs on a job run, not on deploy.** | After a successful `bundle deploy` there are still no control tables. They are created by the first job run. Do not diagnose their absence as a failed deploy. |

### Step 1 — Manual prerequisites

**Artefact used:** [document 01](01_usecase_asset_inventory_and_governance.md), the framing clause and the MANUAL rows of the register in C.1.

Three things cannot be automated and must exist before anything else runs.

| Prerequisite | Action |
|---|---|
| The catalog | Create `br_digital_poc` by hand. Unity Catalog Default Storage rejects `CREATE CATALOG` without a `MANAGED LOCATION`, so this cannot be declared in the bundle. |
| Secret values | Create the `br_digital_poc.config.pgpkey` secret and set its value. The setup notebook in [document 02](02_environment_deployment_and_setup.md) registers the *key placeholder*; it never sets, reads or prints a value. |
| Manual upload folders | Confirm `uc_6/_schema_configs/` and UC7's `output_sample/` and `archive/` — none are created by the notebook. `_schema_configs/` is filled from `BT_Usecase/UC6/onboarding/schema_configs/`. |
| Source files in the upload folder | Fill `workspace_staging_path` from the consolidated per-use-case trees — see the table below. |

#### Filling the upload folder — one tree per use case

Since the v0.0.4 consolidation each use case keeps its docs, onboarding specs and source data
together under `BT_Usecase/<UC>/{docs,onboarding,data}/`. Staging is therefore one tree copy per
use case rather than a hunt across `docs/`, `onboarding/` and `flowx_testing/`.

| Use case | Copy this repository tree | Into | Which the notebook stages into |
|---|---|---|---|
| **UC3** | *(nothing, see below)* | n/a | n/a |
| **UC6** | `BT_Usecase/UC6/data/sample_bundle/` **or** `BT_Usecase/UC6/data/test_fixture/` | `<workspace_staging_path>/UC6/` | `/Volumes/<catalog>/staging/uc_6/raw/` |
| **UC7** | `BT_Usecase/UC7/data/` — `asn_schema/`, `synthetic/`, `tap311_sample.ber` | `<workspace_staging_path>/UC7/` | `/Volumes/<catalog>/landing/uc_7/{asn_schema,raw/<ELEMENT>}/` |

**UC3 needs no upload at all since v0.0.7.** Both of its lanes read connector output that already
exists in Unity Catalog: the streaming lane reads `<catalog>.staging.oracle_excalibur_cdc` (the
multiplexed Debezium landing table, since v0.0.4) and the batch lane reads
`<catalog>.oracle_excalibur_batch.{customer,physical_device,subscriber}`, written by the Lakeflow
Connect Oracle query-based connector. There are no CSVs, no `batch_date=YYYY-MM-DD` folders and no
`uc_3` volume dependency.

`BT_Usecase/UC3/data/` still holds the three Excalibur `*_DDL.csv` governance sheets, which are
column definitions rather than data. *(Historical: while the lanes read files, the CSVs were
generated with `python scripts/generate_uc3_test_data.py --out-dir build/uc3_test_data`, which
wrote the `batch_date=YYYY-MM-DD` layout the routing rules expect, and uploaded into
`/Volumes/<catalog>/staging/uc_3/{streaming,batch}/`.)*

For UC6, upload **one** of the two trees, not both: they share the same six filenames, so the
second would overwrite the first in `staging/uc_6/raw/`. `sample_bundle/` is the customer's
supplied bundle and proves the real-world no-match path; `test_fixture/` is generated and hits
every branch of the decision table.

**One UC7 module is not in the repository.** The notebook validates `EMSC.asn1`, `PSGW.asn1`,
`SGSN.asn1` and `TAP.310.asn1`, but only three of those ship in
`BT_Usecase/UC7/data/asn_schema/`. `SGSN.asn1` must be supplied by the customer straight into the
upload folder or step 4 fails its validation stage. This is gap G-14 in
[document 01](01_usecase_asset_inventory_and_governance.md).

Which of these files may be regenerated and which are customer deliverables is set out in
[`docs/DATA_PROVENANCE_CLASSIFICATION.md`](../../../docs/DATA_PROVENANCE_CLASSIFICATION.md); the
generators are indexed in [`docs/SCRIPTS_GUIDE.md`](../../../docs/SCRIPTS_GUIDE.md).

**Done looks like:** the catalog exists; `pgpkey` resolves; the upload folder holds one tree per
use case in scope, including a customer-supplied `SGSN.asn1` if UC7 is in scope.

**Trap:** the secret scope fallback order is `<catalog>.<schema>`, `<catalog>_<schema>`, `<schema>`,
`<catalog>`. If the secret is created in the wrong spelling it will still resolve, but from a scope
nobody expects — making later rotation miss it.

### Step 2 — Run the setup notebook in `dry_run`

**Artefact used:** [document 02](02_environment_deployment_and_setup.md), the notebook, with `dry_run = true` (its default).

Import `BT_Usecase/_shared/delivery/notebooks/00_setup_uc3_uc6_uc7_environment.py` into the
workspace, set `target_catalog`, `workspace_staging_path` and `use_cases`, and run it.

**Done looks like:** the notebook completes and prints its summary, having written nothing.

**Trap:** confirm `/local_disk0` is writable on your compute before relying on it for zip expansion.
On serverless it may not be — see appendix item 4.

### Step 3 — Review the routing

**Artefact used:** the notebook's summary DataFrame and its printed verdict.

This is the review gate, and the reason step 2 exists. Read the classification of every file, and in
particular:

- **The `_unrouted` count must be zero,** or every entry in it must be a file you agree should not
  be staged. An unrouted file is one the rule table did not recognise.
- **UC3 files with no date in the name** fall back to today's date, with a warning. *(Moot since
  v0.0.7, because UC3 stages no files at all, so its routing rules see nothing. The behaviour is still in
  the notebook and would apply if you staged CSVs to reproduce the old topology.)*
- **UC7 files** must land under the right element folder. A misrouted CDR decodes against the wrong
  ASN.1 module and quarantines every record.
- **All four UC7 `.asn1` modules must be present**, including the customer-supplied `SGSN.asn1`
  that is not in the repository. `GGSN.asn1` and `TAP.311.asn1` may appear if the whole
  `BT_Usecase/UC7/data/asn_schema/` tree was uploaded; they are harmless but are referenced by no
  spec. Gap G-14.

**Done looks like:** every file is routed to a destination you can justify, and you have decided
what to do about any unrouted ones.

**Trap:** `_uc3_table_from` returns `None` outside the three contracted tables, so a typo quarantines
the file rather than silently creating a fourth folder. That is the intended behaviour — but it also
means **adding a UC3 table requires a one-line edit to `UC3_TABLES`.**

### Step 4 — Run the setup notebook for real

**Artefact used:** [document 02](02_environment_deployment_and_setup.md), the notebook, with `dry_run = false`.

**Done looks like:** every schema and volume exists; the directory tree is created; files are copied
and verified by byte size; the validation stage passes with no missing directories; the four UC7
`.asn1` modules are present.

**Traps:**
- The notebook decompresses `.zip` only. `.dat.gz`, `.csv.gz.gpg` and `.gz` pass through
  byte-for-byte and are **never decrypted** — that is the pipeline's job. See the cross-alignment record in [document 04](04_consolidated_rollout_runbook.md), check (h).
- `move_after_copy` deletes the source after a verified copy. Leave it `false` on a first real run.

### Step 5 — Apply container-level tags

**Artefact used:** [document 01](01_usecase_asset_inventory_and_governance.md), the concrete tagging SQL.

Run the `ALTER CATALOG`, `ALTER SCHEMA` and `ALTER VOLUME` tag DDL. This closes gap G-01 and is done
here, once the containers exist and before the tables do.

**Done looks like:** `br_digital_poc.information_schema.catalog_tags`, `schema_tags` and `volume_tags` return
the expected rows.

**Trap:** **qualify every `information_schema` query with the catalog.** An unqualified query
resolves against `current_catalog()` and returns zero rows truthfully, which reads exactly like
failure. See clause C.2.6.

### Step 6 — Onboard the specs

**Artefact used:** the existing onboarding specs, plus the tag decisions from [document 01](01_usecase_asset_inventory_and_governance.md).

Each use case's specs sit alongside its docs and data:

| Use case | Specs |
|---|---|
| UC3 | `BT_Usecase/UC3/onboarding/uc3_excalibur_streaming_cdc.json`, `uc3_excalibur_batch_recon.json` |
| UC6 | `BT_Usecase/UC6/onboarding/uc6_ea_flood_warning.json`, plus `schema_configs/` (five files) |
| UC7 | `BT_Usecase/UC7/onboarding/UC7_cdr_asn_bronze.json` |

Before onboarding, resolve the governance decisions that change the spec content — otherwise you
will onboard twice:

- **Populate UC7's four empty `governance_tags` blocks** (critical finding 1).
- **Resolve UC3's `"?"` classification values** (gap G-07).
- **Decide the sink-tag question** (critical finding 3), so UC6 is onboarded with tags that either
  work or are absent, rather than tags that silently do nothing.

Validate every spec against **both** gates — the JSON schema **and** `spec_validator.py`. They drift,
and only the schema rejects unknown keys.

**Done looks like:** both gates pass for all specs; the onboarding job run succeeds; control-table
rows exist for every flow.

**Trap:** deleting a flow from a spec does **not** deactivate its control-table row. A removed flow
keeps driving the directed acyclic graph until its row is explicitly deactivated.

### Step 7 — Run the jobs

**Artefact used:** the use-case jobs.

Run each use case's job. The first run is what creates the nine control tables via
`setup_control_tables`.

| Use case | Job | Task sequence |
|---|---|---|
| UC3 streaming | **pipeline, not a job** — `bundle run uc3_streaming_cdc_pipeline` | v0.0.4: the pipeline is `continuous: true` for seconds-level CDC latency, so it is started directly and Databricks owns its lifecycle; a continuous update never completes, so no job can wait on it. Seeding is `uc3_seed_job`, tagging is `uc3_governance_job`, and observability is continuous (the pipeline's event log is streamed) — `uc3_streaming_cdc_job` now holds only an on-demand export. See `BT_Usecase/UC3/docs/UC3_MASTER_DOCUMENT.md` §3a.6. |
| UC3 batch | `uc3_batch_recon_job` | `run_pipeline_update` → `heal_physical_device` ‖ `heal_customer` ‖ `heal_subscriber` → `observability_export`. Seeding and tagging are `uc3_seed_job` / `uc3_governance_job`, as for the streaming lane. **v0.0.7:** the lane reads the Lakeflow Connect Oracle tables in `<catalog>.oracle_excalibur_batch`, not the `uc_3` volume. The three `heal_*` tasks are **required**: the reconciliation flows are `execution_mode: "pipeline_audit_only"`, which compares inside the pipeline update but registers no heal lane, so without them the lane reports drift forever and corrects nothing while every run stays green. See `BT_Usecase/UC3/docs/UC3_MASTER_DOCUMENT.md` §3b and §8.3. |
| UC6 | UC6 job (007) | `setup_control_tables`, `onboard_uc6`, `run_pipeline_update`, `apply_governance_uc6`, `observability_export` |
| UC7 | `uc7_cdr_asn_job` (001) | `setup_control_tables`, `onboard_uc7`, `run_pipeline_update`, `observability_export` — **no governance task** |

**Done looks like:** all tasks succeed; Bronze tables are populated; quarantine tables hold only
records you expect.

**Traps:**
- **Do not issue a `bundle deploy` while any of these is running.** See D.0.
- An aggregating materialised view or a `TRUNCATE_AND_LOAD` target can never feed a sink. Both
  validation gates pass and it fails only at pipeline runtime.

### Step 8 — Apply governance

**Artefact used:** the `apply_governance` job tasks, plus the data sensitivity clause in [document 01](01_usecase_asset_inventory_and_governance.md) if enforcement is
adopted.

For UC3 and UC6 this happens inside the job run in step 7. **For UC7 it does not happen at all** —
the task does not exist. Until critical finding 1 is resolved, either add the task or run the tag
DDL by hand.

If critical finding 2 was decided in favour of enforcement, apply the column masks and row filters
now — **after** the pipeline update, never before, since the tables must exist first.

**Done looks like:** `br_digital_poc.information_schema.table_tags` and `column_tags` return the expected rows
for all three use cases, UC7 included.

**Trap:** governed tag policy violations fail **here**, at the apply task, not at spec validation.
A spec that validated cleanly can still fail this step.

### Step 9 — Deploy the application with the Beta banner

**Artefact used:** the React components and release-stage clause in [document 03](03_platform_best_practice_and_naming_standards.md).

1. Resolve critical findings 4 and 5 first — correct `framework_version` to `1.7.4` and reconcile
   `support_contact`. The banner's entire purpose is to state the truth about the deployment.
2. Add `BetaNotice.jsx` and apply the five wiring points from A.5.2. Ship the banner **non-sticky**
   (critical finding 6).
3. Run `node --check` on every edited file under `web/src`.
4. Run **`npm run build`** in `databricks-app/web`.
5. Run `databricks bundle deploy`, then **`databricks bundle run`**.

**Done looks like:** the header shows the Beta pill; the banner appears below the header on every
route; dismissal lasts the session and the pill remains; the provenance block reads 1.7.4.

**Traps:** all three application traps from D.0 apply to this step, and they are the reason it has
five sub-steps rather than one.

### Step 10 — Verify

**Artefact used:** all three parts.

| Check | How | Source |
|---|---|---|
| Volumes and directory trees exist | The notebook's validation stage | [document 02](02_environment_deployment_and_setup.md) |
| Files routed correctly, nothing unrouted | The notebook's summary | [document 02](02_environment_deployment_and_setup.md) |
| Container tags applied | `br_digital_poc.information_schema.{catalog,schema,volume}_tags` | [document 01](01_usecase_asset_inventory_and_governance.md) (tagging SQL) |
| Table and column tags applied for **all three** use cases | `br_digital_poc.information_schema.{table,column}_tags` | [document 01](01_usecase_asset_inventory_and_governance.md) (tagging) |
| UC7 specifically has tags | Same query, filtered to the four UC7 tables | Critical finding 1 |
| Bronze row counts are non-zero and quarantine is as expected | Query the tables | [document 01](01_usecase_asset_inventory_and_governance.md) (asset register) |
| Beta pill and banner render, light and dark | Open the app in both themes | [document 03](03_platform_best_practice_and_naming_standards.md) |
| Provenance block reads 1.7.4 | Open the About panel | Critical finding 4 |
| No grant gives schema-wide Bronze access | Review grants | [document 01](01_usecase_asset_inventory_and_governance.md) (grant patterns) |

**Done looks like:** every row above checks out, and the two governance decisions (critical findings
2 and 3) are recorded — whichever way they went.

---

---

## Cross-alignment verification record

Eight checks were performed between the notebook in [document 02](02_environment_deployment_and_setup.md) and the register in [document 01](01_usecase_asset_inventory_and_governance.md), plus the
underlying repository. Each was verified against the source, not inferred from the specialist
outputs. Where the two disagreed, the repository's build contracts decided it.

| Check | Question | Finding | Resolution |
|---|---|---|---|
| **(a)** | Does every schema, volume and path in the notebook's `TOPOLOGY` match the asset register? | **Match, with one addition and one absence, both handled below.** `TOPOLOGY` declares SHARED (`config`, `observability`; volumes `app_logs` with `streaming_cdc`/`batch_recon`, and `wheels/0.0.3`), UC3 (`staging`, `bronze`; volume `uc_3`), UC6 (`bronze`, `silver`, `gold`, `staging`; volume `uc_6`), UC7 (`bronze`, `landing`; volume `uc_7`). All correspond to register entries. | No change. The register in C.1 was written to match `TOPOLOGY` exactly, with provenance marked per row. |
| **(b)** | The notebook creates `<catalog>.staging._unrouted`, which appears in no build contract. | **Confirmed new.** Created at notebook line 1136 and used as the quarantine destination for unclassified files. It exists in no bundle resource and in no specialist register. | **Added to the register as an object this package introduces, and flagged for approval.** It is a managed volume, so it is cheap and removable. **Alternative if a new volume is not wanted:** make it a subfolder of an existing volume, for example `staging/uc_3/_unrouted/`. That avoids a new securable at the cost of mixing quarantine into a use-case volume — which is why the separate volume was preferred. **Decision required.** |
| **(c)** | The notebook creates `silver`, `gold`, `landing`, `config` and `observability`; the register lists some as pre-existing and some as bundle-created. | **Both descriptions are correct, for different workspaces.** On the established `metaflow_v7` workspace `config`, `observability`, `bronze` and `staging` already exist and the notebook finds them; `silver`, `gold` and `landing` may or may not, depending on whether the UC6 and UC7 pipelines have run. On a fresh workspace the notebook creates all of them. | **Resolved by stating the rule rather than the state:** every statement is `CREATE SCHEMA IF NOT EXISTS`, so the notebook **finds** what exists and **creates** what does not, and the outcome is identical either way. The register marks the established-workspace position and cross-references this check. UC6's `silver` and `gold` are consequently created in two places — the notebook and the pipeline-adjacent DDL — which is harmless for the same reason. |
| **(d)** | The notebook registers a `security` schema for `pii_encryption_key`, absent from the register. | **Confirmed forward-looking and unused.** Notebook lines 360–361. No UC3, UC6 or UC7 spec references `pii_encryption_key`; no column encryption is implemented anywhere in the framework. | **Flagged as optional; recommend dropping it** unless column encryption is planned. It is deliberately excluded from the register in C.1, because listing an asset no use case uses would misrepresent the estate. Listed in the appendix as item 1. |
| **(e)** | Do the notebook's derived folder names match the documented conventions and the specs? | **Exact match on all of them.** `uc_3`, `uc_6`, `uc_7` match the `uc_<N>` volume convention in C.3.2. `batch_date=` matches the Hive-style partition folder. `_schemas/<source>/` and `_extracted/<source>/` match both the convention and the six UC6 source paths in the spec. `asn_schema/` matches the UC7 spec's `asn1_schema_path` directory. | No change required. |
| **(f)** | Does the notebook's wheel version match the register and `pyproject.toml`? | **Match.** Notebook line 228 sets `FRAMEWORK_WHEEL_VERSION = "0.0.3"`; `pyproject.toml` line 3 is `version = "0.0.3"`; the UC7 spec's provenance names `/Volumes/br_digital_poc/config/wheels/0.0.3/.internal/flowx-0.0.3-py3-none-any.whl`. | No change. **Noted as a maintenance obligation:** the constant is hand-managed and must be bumped per release. Appendix item 2. |
| **(g)** | Do the `.asn1` filenames the notebook validates match the UC7 spec's `asn1_schema_path` values? | **Exact match on all four.** The notebook checks `{EMSC.asn1, PSGW.asn1, SGSN.asn1, TAP.310.asn1}` at line 1466. The spec's four `asn1_schema_path` values are `/Volumes/{{catalog}}/landing/uc_7/asn_schema/` plus, respectively, `EMSC.asn1`, `PSGW.asn1`, `SGSN.asn1` and `TAP.310.asn1`. | No change. **`TAP.310.asn1` is deliberately version-specific** and is not interchangeable with `TAP.311.asn1`: the TAP version is proven from the payload (specification version 3, release version 10), and 3.11 drops the `valueAddedService` CHOICE arm. Do not "upgrade" this filename. |
| **(h)** | The UC6 `EE_` pattern matches a `.csv` name, but the notebook stages a `.csv.gz.gpg` file. Is this a mismatch? | **Not a mismatch — the two patterns describe different points in the pipeline.** The flow declares `"file_pattern": "EE_*-REQUEST_*.csv"` **and** `"zip_file_pattern": "EE_*-REQUEST_*[Oo][Ff]*.csv.gz.gpg"`, with `pre_extraction_decryption` configured and a `target_volume_path` of `_extracted/ea_request/`. The notebook stages the still-encrypted `.csv.gz.gpg`; the framework decrypts and decompresses it into `_extracted/`; Auto Loader then reads the resulting `.csv` via `file_pattern`. The source `path` for all six UC6 flows is under `_extracted/`, confirming the framework reads post-decryption names throughout. | **No change — but recorded here because it looks like a defect to a reviewer.** `file_pattern` is the **post**-decryption name; `zip_file_pattern` is the **pre**-decryption name. The other five feeds are `*.dat.gz`, which are decompressed but not decrypted, so their two patterns look more alike and the distinction is less visible. |

### Editorial resolutions

Two further points where the specialist outputs did not agree, resolved here rather than presented
twice:

| Point | Disagreement | Resolution |
|---|---|---|
| Support contact | The application defaults to `data-platform@example.com`; the Beta disclaimer directs to Hoonartek. | Resolved in favour of **Hoonartek**, as the disclaimer is the change-controlled text. Raised as critical finding 5 because it requires a code change, not just an editorial one. |
| Framework version | The disclaimer's provenance block states 1.7.4; the application's `settings.py` defaults to 1.5.0. | Resolved in favour of **1.7.4**, the repository's actual version. Raised as critical finding 4. The document uses 1.7.4 throughout, and [document 03](03_platform_best_practice_and_naming_standards.md) notes that the code must be corrected to match. |

---

---

## Appendix — open assumptions requiring confirmation

Eleven items requiring a human decision before or during rollout. The first six come from the
notebook, the remainder from the governance inventory. Each names who should decide.

| # | Item | Detail | Decision needed | Owner |
|---|---|---|---|---|
| 1 | The `security` schema | Registered for a forward-looking `pii_encryption_key` that no UC3, UC6 or UC7 spec uses. | **Drop it** unless column encryption is planned. See the cross-alignment record in [document 04](04_consolidated_rollout_runbook.md), check (d). | Framework Engineering Lead |
| 2 | Wheel version constant | `FRAMEWORK_WHEEL_VERSION = "0.0.3"` is hand-managed and must be bumped per release. | Confirm the release process includes this, or derive it from `pyproject.toml`. | Framework Engineering Lead |
| 3 | UC3 `batch_date` fallback | A file carrying no date falls back to today's date, with a warning. | Confirm this is correct for your load pattern, or require dates in filenames. | Data Engineering Lead |
| 4 | `/local_disk0` for zip expansion | Used for archive expansion and the Workspace Export API fallback. | **Confirm it is writable on your compute.** On serverless it may not be; switch to `tempfile.mkdtemp()` if not. | Data Engineering Lead |
| 5 | Duplicate `silver` and `gold` creation | Created by both the notebook and the pipeline-adjacent DDL. | No action needed — both are `IF NOT EXISTS`. Recorded so it is not later reported as a defect. | — (informational) |
| 6 | The `_unrouted` quarantine volume | A new object in no build contract. | **Approve the new volume, or choose the subfolder alternative.** See the cross-alignment record in [document 04](04_consolidated_rollout_runbook.md), check (b). | Data Architecture Lead |
| 7 | UC3 batch credential columns | Four columns are nulled at source on the **streaming** path. **v0.0.7: the batch path now nulls them in its `transformation_sql`** (`CAST(NULL AS STRING) AS <col>` on `esn_pin`, `blacklist_password`, `acc_password`, `imei_black_list_pass`, `gur_cr_card_no`), which is where a transformation flow expresses shaping; `data_standardization_sql` is a `source_config` key and no longer applies. | **Confirm on the live tables after the next run.** The spec is correct by inspection, but the end-to-end run had not been verified at the time of writing. See C.4.2 and `BT_Usecase/UC3/docs/UC3_MASTER_DOCUMENT.md` §7.2. | Data Governance Lead |
| 8 | Enforcement decision | No column masks or row filters exist. | Decide whether Beta accepts descriptive-only governance, and **record the decision either way**. Gap G-11, critical finding 2. | Data Governance Lead with Information Security |
| 9 | Sink tagging | Four UC6 sinks declare tags the engine cannot apply. | Extend the engine, or reject the tags at validation. Silence is not an option under `AGENTS.md`. Gap G-04, critical finding 3. | Framework Engineering Lead |
| 10 | Secret rotation | One passphrase covers UC6 ingress and egress; no rotation record exists. | Decide whether to split the passphrases, and set a rotation schedule. Gap G-13. | Information Security |
| 11 | Missing `SGSN.asn1` | The UC7 spec and the notebook's validation both name `SGSN.asn1`, which is in no part of the repository. `GGSN.asn1` and `TAP.311.asn1` are present but referenced by no spec. | **Confirm the customer supplies `SGSN.asn1`** into the upload folder before step 4, or UC7 fails validation. Gap G-14. | Data Engineering Lead |

### Adding a UC3 table — a note for whoever comes next

`_uc3_table_from` returns `None` for anything outside the three contracted tables, so an unrecognised
name is quarantined rather than silently given a fourth folder. This is deliberate and correct. It
does mean that **adding a fourth UC3 table requires a one-line edit to `UC3_TABLES`** in the
notebook, alongside the spec change. Without it, the new table's files will land in `_unrouted/` and
the run will report a warning rather than staging them.

---


---

*End of the delivery set.*
