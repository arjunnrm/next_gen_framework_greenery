# 01 — Use Case Asset Inventory and Governance

**FlowX (NextGen Metadata Framework) · UC3, UC6, UC7**
**Framework release:** v1.7.4 · **Wheel artefact version:** 0.0.3
**Document date:** 5 September 2026
**Status:** For review and sign-off prior to rollout
**Audience:** Data Governance Lead, Information Security, Data Platform Architects

---

## What this document is

The complete register of every Unity Catalog asset the three use cases own, how each is tagged, how
sensitive it is, who may read it, and where governance is currently incomplete.

It is a description of the estate **as it actually stands**, not a target state. Every row was
verified against the repository. Where an object is a manual prerequisite rather than a
bundle-declared resource, it says so, because that distinction determines whether a fresh deployment
will create it.

For how the assets are provisioned and populated, see document 02. For the naming conventions the
register follows, see document 03. For the order in which the whole estate is stood up, see
document 04.

## The three use cases in one line each

| Use case | One-line description | Sensitivity |
|---|---|---|
| **UC3** | Excalibur telecoms reference data — three tables (`physical_device`, `customer`, `subscriber`) ingested as a streaming CDC feed and reconciled by a parallel batch path. | CONFIDENTIAL, with RESTRICTED columns (credentials, a card number) |
| **UC6** | Environment Agency flood warning — six GPG-encrypted or gzipped customer feeds landed, decrypted, conformed to Silver and Gold, and egressed as four encrypted files. | CONFIDENTIAL — inherently a vulnerable-person register |
| **UC7** | Call detail records — four network elements (EMSC, PSGW, SGSN, TAP) decoded from BER-encoded ASN.1 into Bronze, with per-table quarantine. | RESTRICTED — MSISDN, IMSI, IMEI, cell-site location |

## Governance findings that need a decision

Three of the six cross-cutting findings in this delivery set are governance findings, and they
belong to the owner of this document. They are stated here in full; the other three are in
document 03 (interface correctness and layout).

| # | Finding | Evidence | Recommended action | Owner role |
|---|---|---|---|---|
| **1** | **UC7 is entirely ungoverned** (gap G-03). All four UC7 ingestion flows declare `"governance_tags": {}`, and `resources/uc7/uc7_cdr_asn_job.yml` is the only one of the three use-case jobs with **no** `apply_governance` task. The most sensitive data in the estate — CDRs carrying MSISDN, IMSI, IMEI and cell-site location — carries no tags at all. | `BT_Usecase/UC7/onboarding/UC7_cdr_asn_bronze.json` lines 51, 91, 136, 181; UC7 job task list is `setup_control_tables`, `onboard_uc7`, `run_pipeline_update`, `observability_export`. Both UC3 jobs and the UC6 job carry an `apply_governance_*` task. | Populate `governance_tags` on all four UC7 flows and add an `apply_governance_uc7` task to the job, modelled on `apply_governance_uc3`. Treat as a rollout blocker for UC7. | Data Governance Lead |
| **2** | **No column masks or row filters exist anywhere** (gap G-11). Tags are applied but never *enforced*. Classification is descriptive metadata only; nothing in the estate restricts access to a RESTRICTED column. | No `CREATE FUNCTION` mask or `SET ROW FILTER` statement exists in the repository. | Decide explicitly whether Beta accepts descriptive-only governance. If not, implement the masks in the data sensitivity clause in [document 01](01_usecase_asset_inventory_and_governance.md) below and schedule them as post-pipeline DDL. Record the decision either way. | Data Governance Lead with Information Security |
| **3** | **Sink governance tags are silently discarded** (gap G-04). Four UC6 sink flows declare a complete `table_tags` block (`data_classification: confidential`, `pii: true`, and four more keys), but `governance/tags.py` iterates only `ingestion_rows` and `transformation_rows` and emits only `ALTER TABLE`. A sink writes files to a volume, so there is no table to alter. The tags are accepted at validation and then do nothing. | `src/flowx/lakeflow_framework/governance/tags.py` line 181; four sink flows in `BT_Usecase/UC6/onboarding/uc6_ea_flood_warning.json`. | This is a silent no-op of exactly the kind `AGENTS.md` prohibits. Either extend the engine to tag the sink's volume, or **reject** `governance_tags` on sink flows at validation with a message naming the limitation. Do not leave it silent. | Framework Engineering Lead |

Findings 1 and 3 have small, well-understood fixes and should simply be done. Finding 2 is a genuine
decision with cost attached, and is the one that warrants discussion rather than action.

---

## A note on the catalog name

Throughout this delivery set the Unity Catalog name is **always a parameter, never a literal**.
Two names appear, and the distinction matters:

| Name | What it is |
|---|---|
| `flowx` | The **live catalog** on the `metaflow_v7` workspace. Every asset the repository currently owns lives here, and the asset register in document 01 describes this estate as it actually stands. |
| `bt_digital_poc` | A **worked example** of a different deployment target, used to show how the same artefacts are pointed at another catalog without editing them. |

Wherever a path is written as `/Volumes/<catalog>/...`, substitute whichever applies. In the setup
notebook this is the `target_catalog` widget; in a bundle it is `${var.catalog}`; in an onboarding
spec it is the `{{catalog}}` token. So the UC7 landing path resolves as:

```text
/Volumes/flowx/landing/uc_7/raw/EMSC/            <- current metaflow_v7 estate
/Volumes/bt_digital_poc/landing/uc_7/raw/EMSC/   <- e.g. a POC deployment
```

**One caveat that is not cosmetic.** `resources/uc7/*.yml` currently hardcodes `catalog: flowx`
rather than using `${var.catalog}`. Until that is parameterised, UC7 cannot be deployed into
`bt_digital_poc` or any other catalog by changing a variable alone. This is tracked as gap G-05 in
document 01, and it is a prerequisite for any catalog migration.

---

## The delivery set

| # | Document | Covers |
|---|---|---|
| 01 | [Use case asset inventory and governance](01_usecase_asset_inventory_and_governance.md) | Asset register, tagging strategy, data sensitivity, access control, governance gaps |
| 02 | [Environment deployment and setup](02_environment_deployment_and_setup.md) | The setup and staging notebook, its design, and how to run it |
| 03 | [Platform best practice and naming standards](03_platform_best_practice_and_naming_standards.md) | Naming conventions, Beta release governance, UI components, build and deploy practice |
| 04 | [Consolidated rollout runbook](04_consolidated_rollout_runbook.md) | The ordered end-to-end sequence, cross-alignment record, open assumptions |

Each document stands alone. Where one depends on another it links rather than repeats.

---

## The inventory

### 0 Framing — what can and cannot be automated

Three constraints shape everything in this part. Stating them first prevents the register below
being read as a list of things the bundle creates, which it is not.

1. **The catalog cannot be declared in YAML.** Unity Catalog Default Storage rejects
   `CREATE CATALOG` without a `MANAGED LOCATION`. This was attempted and reverted on
   2 September 2026. The catalog is, and remains, a manual prerequisite.
2. **Only six volumes are bundle-managed.** They are `flowx.config.wheels`, the onboarding-specs
   volume, and four `flowx_sample` volumes (`landing`, `exports`, `observability`,
   `sample_configs`). **Every use-case landing volume — `uc_3`, `uc_6`, `uc_7` — is a manual
   prerequisite.** This is precisely the gap the setup notebook in [document 02](02_environment_deployment_and_setup.md) closes.
3. **Tags are applied but never enforced.** No column masks and no row filters exist anywhere in
   the estate. Classification is descriptive metadata; it restricts nothing. This is gap G-11 and
   critical finding 2.

A fourth point is worth flagging because it surprises people: **the nine control tables are created
at runtime by the `setup_control_tables` job task, not by `bundle deploy`.** A successful deploy
leaves you with no control tables. They appear on the first job run.

### 1 Asset register

Every row is marked with its provenance: **PRE-EXISTING** (already in the workspace, reused),
**NEWLY created** (by this package or by a bundle deploy), or **MANUAL** (an operator prerequisite).

#### C.1.1 Platform-wide assets

| Asset | Type | Provenance | Notes |
|---|---|---|---|
| `flowx` | Catalog | **MANUAL** | Cannot be declared in YAML. Migration from the Metaflow-era name is still pending. |
| `flowx.config` | Schema | PRE-EXISTING | Holds the control tables and the wheels volume. |
| `flowx.dev` | Schema | PRE-EXISTING | Development scratch. |
| `flowx.flowx_sample` | Schema | PRE-EXISTING | Sample and bootstrap assets. |
| `flowx.bronze` | Schema | PRE-EXISTING | **Shared by all three use cases** — see gap G-09 and the grant caution in C.4.4. |
| `flowx.staging` | Schema | PRE-EXISTING | Landing volumes for UC3 and UC6. |
| `flowx.observability` | Schema | PRE-EXISTING | Observability exports. |
| `flowx.config.wheels` | Volume | Bundle-managed | Framework wheel, version-scoped: `0.0.3/`. |
| `flowx.config.onboarding_specs` | Volume | Bundle-managed | Specs uploaded by the application. |
| `flowx.observability.app_logs` | Volume | Bundle-managed | Subfolders `streaming_cdc/`, `batch_recon/`. |
| `flowx.config.pgpkey` | Secret | **MANUAL** | The GPG passphrase. Covers both UC6 ingress and egress — see gap G-13. |
| FlowX application | Databricks App | Bundle-managed | The spec builder front end. [document 03](03_platform_best_practice_and_naming_standards.md) applies here. |
| `onboarding_job` | Job | Bundle-managed | Generic onboarding job; use-case jobs delegate to it. |
| `framework_config_onboarding_job` | Job | Bundle-managed | Framework configuration onboarding. |
| Preflight function | UC function | Bundle-managed | Pre-run validation. |

#### C.1.2 Control tables — created at runtime, not at deploy

Nine tables in `flowx.config`, created by the `setup_control_tables` task on the **first job run**.
A `bundle deploy` alone does not create them. This is the most common cause of a "the tables are
missing after a successful deploy" report.

| # | Control table | Holds |
|---|---|---|
| 1 | Dataflow group registry | Group identity and activation state |
| 2 | Ingestion flow definitions | Source-to-Bronze flow rows, including `governance_tags_json` |
| 3 | Transformation flow definitions | Bronze-onward flow rows, including `governance_tags_json` |
| 4 | Reconciliation flow definitions | Reconciliation flow rows |
| 5 | Data quality rules | Per-flow rule expressions |
| 6 | Schema configuration | Column definitions and normalisation settings |
| 7 | Run and audit log | Execution history |
| 8 | Observability metrics | Pipeline metrics |
| 9 | Reconciliation results | Reconciliation outcomes |

> Only rows 2 and 3 carry tags that the `apply_governance` task will act on. Row 4 does not — see
> gap G-04 and critical finding 3.

#### C.1.3 UC3 — Excalibur telecoms reference data

| Asset | Type | Provenance | Notes |
|---|---|---|---|
| `flowx.staging.uc_3` | Volume | **MANUAL** | Created by the setup notebook in [document 02](02_environment_deployment_and_setup.md). |
| `uc_3/streaming/<table>/` | Path | Notebook | Three tables: `physical_device`, `customer`, `subscriber`. |
| `uc_3/batch/<table>/batch_date=<date>/` | Path | Notebook | Hive-style partition folder. |
| `BT_Usecase/UC3/{docs,onboarding,data}/` | Repository tree | — | Docs, the two specs, and the three `*_DDL.csv` sheets. Staged data is generated to `build/uc3_test_data/` — see C.1.6. |
| `flowx.staging.*_stream` (x3) | Tables | Pipeline | Streaming staging tables. |
| `flowx.bronze.*` CDC targets (x3) | Tables | Pipeline | SCD1 and SCD2 targets. |
| `flowx.staging.*_batch` (x3) | Tables | Pipeline | Batch staging tables. |
| UC3 streaming CDC pipeline | Pipeline | Bundle-managed | |
| UC3 batch reconciliation pipeline | Pipeline | Bundle-managed | |
| `uc3_streaming_cdc_job` | Job | Bundle-managed | **Has** `apply_governance_uc3`. |
| `uc3_batch_recon_job` | Job | Bundle-managed | **Has** `apply_governance_uc3_recon`. |
| Observability export job | Job | Bundle-managed | |
| Two dataflow groups | Control rows | Runtime | Streaming and batch. |
| `flowx.uc3_bronze` | Schema | PRE-EXISTING | **Orphaned and inert** — see gap G-02. |

#### C.1.4 UC6 — Environment Agency flood warning

| Asset | Type | Provenance | Notes |
|---|---|---|---|
| `flowx.staging.uc_6` | Volume | **MANUAL** | Created by the setup notebook in [document 02](02_environment_deployment_and_setup.md). |
| `uc_6/raw/`, `archive/`, `output/`, `_schemas/`, `_extracted/` | Paths | Notebook | Five subfolders, plus per-source folders beneath `_schemas/` and `_extracted/`. |
| `uc_6/_schema_configs/` | Path | **MANUAL** | Schema-configuration JSON uploaded by hand, from `BT_Usecase/UC6/onboarding/schema_configs/`. |
| `BT_Usecase/UC6/{docs,onboarding,data}/` | Repository tree | — | Docs, the spec and its `schema_configs/`, and both source-data trees — see C.1.6. |
| `flowx.silver` | Schema | Notebook / pipeline | See the cross-alignment record in [document 04](04_consolidated_rollout_runbook.md), check (c). |
| `flowx.gold` | Schema | Notebook / pipeline | See the cross-alignment record in [document 04](04_consolidated_rollout_runbook.md), check (c). |
| `flowx.bronze.uc6_*` (x6) | Tables | Pipeline | One per source feed. |
| Silver objects (x4) | Tables / views | Pipeline | Conformed layer. |
| Gold materialised views (x2) | MVs | Pipeline | Aggregated layer. |
| Egress sinks (x4) | Sinks | Pipeline | Write encrypted files to `uc_6/output/`. **Declared tags are never applied** — gap G-04. |
| UC6 pipeline (008) | Pipeline | Bundle-managed | |
| UC6 job (007) | Job | Bundle-managed | **Has** `apply_governance_uc6`. |
| One dataflow group | Control rows | Runtime | |
| `flowx.config.pgpkey` | Secret | **MANUAL** | Shared ingress and egress passphrase. |

The six source feeds are `ea_request`, `css_account`, `css_account_address`, `css_subscription`,
`jt_customer` and `excalibur_address`.

#### C.1.5 UC7 — Call detail records

| Asset | Type | Provenance | Notes |
|---|---|---|---|
| `flowx.landing` | Schema | Notebook / pipeline | See the cross-alignment record in [document 04](04_consolidated_rollout_runbook.md), check (c). |
| `flowx.landing.uc_7` | Volume | **MANUAL** | Created by the setup notebook in [document 02](02_environment_deployment_and_setup.md). |
| `uc_7/raw/<ELEMENT>/` | Paths | Notebook | Four elements: `EMSC`, `PSGW`, `SGSN`, `TAP`. |
| `uc_7/asn_schema/` | Path | Notebook | The four `.asn1` module files. |
| `uc_7/_schemas/` | Path | Notebook | Auto Loader schema checkpoints. |
| `uc_7/output_sample/`, `archive/` | Paths | **MANUAL** | Not created by the notebook. |
| `BT_Usecase/UC7/{docs,onboarding,data}/` | Repository tree | — | Docs including the test report, the spec, and `asn_schema/` + `synthetic/` — see C.1.6 and gap G-14. |
| `flowx.bronze.emsc_cdr_raw`, `psgw_cdr_raw`, `sgsn_cdr_raw`, `tap310_raw` | Tables | Pipeline | The four Bronze targets. |
| Quarantine tables (x4) | Tables | Pipeline | One per Bronze table. |
| UC7 pipeline (001) | Pipeline | Bundle-managed | |
| `uc7_cdr_asn_job` (001) | Job | Bundle-managed | **NO `apply_governance` task** — gap G-03, critical finding 1. |
| One dataflow group | Control rows | Runtime | |

**On the deliberate SMSC and MMSC exclusion.** Two further network elements exist in the source
estate and are intentionally *not* onboarded. Their payloads are CSV text, not ASN.1 — SMSC is a
single-line 48-field quoted CSV, MMSC a 70-plus column CSV — there is no `SMSC.asn1` module in
`asn_schema/` at all, and both fail to decode against every available module with a
`DecodeTagError`. Onboarding them with `source_type: asn1` would quarantine 100 per cent of their
rows while reporting success. They are tracked as open items in
`BT_Usecase/UC7/docs/UC7_CDR_ASN_Test_Report.md`.

#### C.1.6 Repository source-data assets, and which may be regenerated

The register above covers Unity Catalog securables. This clause covers the other half of the
estate: the source files in the repository that get staged **into** those securables. It exists
because the two halves have opposite risk profiles — a Unity Catalog table can be rebuilt from its
source, but a customer-supplied source file that is regenerated is gone.

Since the v0.0.4 consolidation each use case keeps its documentation, its onboarding specs and its
source data together in one place:

```text
BT_Usecase/UC3/{docs,onboarding,data}/
BT_Usecase/UC6/{docs,onboarding,data}/
BT_Usecase/UC7/{docs,onboarding,data}/
BT_Usecase/_shared/delivery/          <- this delivery set and its setup notebook
```

Every data asset is marked **[Customer-Provided]** or **[Simulated]**. The classification is not a
judgement about realism — it is a statement about whether a repository script can reproduce the
file. **A [Customer-Provided] file has no generator and must never be regenerated, overwritten or
"refreshed".**

| Use case | Asset | Class | Basis |
|---|---|---|---|
| **UC3** | `BT_Usecase/UC3/data/CUSTOMER_DDL.csv`, `SUBSCRIBER_DDL.csv`, `PHYSICAL_DEVICE_DDL.csv` | **[Customer-Provided]** | The three Excalibur governance sheets. `scripts/generate_uc3_test_data.py` **reads** them to learn column names, types and governance flags; it never writes them. A generator's input is not its output. |
| **UC3** | `build/uc3_test_data/**` | **[Simulated]** | Written by `scripts/generate_uc3_test_data.py --out-dir build/uc3_test_data`. `build/` is gitignored scratch space, so the CSVs are generated on demand rather than stored. Fully reproducible. |
| **UC6** | `BT_Usecase/UC6/data/sample_bundle/**` | **[Customer-Provided]** | The supplied `uc_6_poc_bundle.zip`, sanitised by the customer at source per the Flood Warning System POC interface specification. No repository script generates it. |
| **UC6** | `BT_Usecase/UC6/data/test_fixture/**` | **[Simulated]** | Written by `scripts/generate_uc6_test_data.py`. It exists because the supplied bundle has no postcode overlap and no CSS join-key overlap, so that bundle can only ever exercise the no-match path. |
| **UC7** | `BT_Usecase/UC7/data/asn_schema/*.asn1` | **[Customer-Provided]** | Real ASN.1 protocol module definitions. `scripts/generate_synthetic_ber.py` reads them as input. |
| **UC7** | `BT_Usecase/UC7/data/tap311_sample.ber` | **[Customer-Provided]** | Supplied sample payload; written by no generator. |
| **UC7** | `BT_Usecase/UC7/data/EE_*.csv.gz.gpg` | **[Customer-Provided]** | Supplied encrypted EA request file. |
| **UC7** | `BT_Usecase/UC7/data/synthetic/*.ber` | **[Simulated]** | Written by `scripts/generate_synthetic_ber.py`, ten records per protocol, deterministic. |

**Both UC6 trees are retained deliberately.** The supplied bundle proves the real-world no-match
path; the generated fixture proves every branch of the decision table. Deleting either loses a
distinct test, so the register lists them as two assets rather than one with a preferred variant.

**One UC7 module is a genuine gap, not a classification question.** The setup notebook validates
that `EMSC.asn1`, `PSGW.asn1`, `SGSN.asn1` and `TAP.310.asn1` are present in
`landing/uc_7/asn_schema/`, and the UC7 spec's four `asn1_schema_path` values name those same four
modules. **`SGSN.asn1` is in no part of this repository** and must be supplied by the customer
straight into the upload folder. Conversely `GGSN.asn1` and `TAP.311.asn1` *are* in the repository
but no spec references them; they are generator inputs, not pipeline schemas. This is recorded as
gap G-14.

The full classification, with per-file evidence, is in
[`docs/DATA_PROVENANCE_CLASSIFICATION.md`](../../../docs/DATA_PROVENANCE_CLASSIFICATION.md), and
every script named above is indexed in
[`docs/SCRIPTS_GUIDE.md`](../../../docs/SCRIPTS_GUIDE.md). This clause summarises those two
documents for the use-case assets only; where they disagree, they are authoritative.

#### C.1.7 Summary counts

| Category | UC3 | UC6 | UC7 | Shared | Total |
|---|---|---|---|---|---|
| Volumes | 1 | 1 | 1 | 3 | 6 |
| Tables | 9 | 12 | 8 | 9 control | 38 |
| Pipelines | 2 | 1 | 1 | — | 4 |
| Jobs | 3 | 1 | 1 | 2 | 7 |
| Dataflow groups | 2 | 1 | 1 | — | 4 |
| Secrets | 0 | 1 | 0 | — | 1 |

Schemas are deliberately omitted from the count because they overlap: `flowx.bronze` is shared by
all three use cases, which is itself gap G-09.

### 2 Tagging strategy

#### C.2.1 The framework's own route is the preferred one

FlowX has a first-class tagging mechanism, and it should be used in preference to hand-written DDL:

```text
onboarding spec  ->  governance_tags
                 ->  governance_tags_json column in the control tables
                 ->  the apply_governance job task
                 ->  ALTER TABLE ... SET TAGS DDL
```

Two rules govern the spec attribute, and both are enforced at validation:

- **Exactly two keys are allowed.** `ALLOWED_GOVERNANCE_TAGS_KEYS = {"column_tags", "table_tags"}`.
  Anything else is rejected.
- **Values must be strings.** `"pii": true` is rejected; it must be `"pii": "true"`.

#### C.2.2 Three structural limits

1. **Only ingestion and transformation flows are tagged.** `governance/tags.py` iterates
   `ingestion_rows + transformation_rows`. Reconciliation flows and sinks are not tagged — the
   latter silently, which is gap G-04.
2. **Tagging is opt-in and silent when absent.** A flow with no `governance_tags` produces no
   warning. UC7's four empty blocks pass validation cleanly.
3. **The DDL needs its own job task.** Tags are not applied by the pipeline. Without an
   `apply_governance` task in the job, spec tags are inert — which is exactly UC7's position.

#### C.2.3 Reconciling the proposed vocabulary with the repository's actual one

The proposed enterprise keys use title case; the repository uses lowercase snake case. The
repository's vocabulary wins, because changing it would orphan the tags already applied.

| Proposed key | Actual repository key | Status |
|---|---|---|
| `Environment` | `environment` | In use |
| `Owner` | `owner` | In use |
| `UseCase_ID` | `use_case` | In use |
| `Data_Sensitivity` | `data_classification` | In use |
| `Cost_Center` | `cost_centre` | **NEW** |
| `Created_By` | `created_by` | **NEW** |

#### C.2.4 Canonical keys and value domains

| Key | Value domain | Status |
|---|---|---|
| `environment` | `poc`, `dev`, `test`, `prod` | In use |
| `owner` | Team identifier, e.g. `business_data_and_ai` | In use |
| `use_case` | `uc3`, `uc6`, `uc7` | In use |
| `data_classification` | `public`, `internal`, `confidential`, `restricted` | In use |
| `source_system` | Source identifier, or `derived` | In use |
| `pii` | `"true"`, `"false"` — string, never boolean | In use |
| `cost_centre` | Finance cost-centre code | **NEW** — gap G-10 |
| `created_by` | Owning individual or team | **NEW** — gap G-10 |
| `retention_policy` | Retention identifier | **NEW** — gap G-10 |

#### C.2.5 Container-level tag DDL

The framework tags tables only. Catalog, schema and volume tags must be applied by hand — gap G-01.

```sql
ALTER CATALOG flowx SET TAGS ('environment' = 'poc', 'owner' = 'business_data_and_ai');

ALTER SCHEMA flowx.bronze  SET TAGS ('environment' = 'poc', 'data_classification' = 'confidential');
ALTER SCHEMA flowx.landing SET TAGS ('environment' = 'poc', 'data_classification' = 'restricted');
ALTER SCHEMA flowx.silver  SET TAGS ('environment' = 'poc', 'data_classification' = 'confidential');
ALTER SCHEMA flowx.gold    SET TAGS ('environment' = 'poc', 'data_classification' = 'confidential');

ALTER VOLUME flowx.landing.uc_7 SET TAGS ('use_case' = 'uc7', 'data_classification' = 'restricted');
ALTER VOLUME flowx.staging.uc_6 SET TAGS ('use_case' = 'uc6', 'data_classification' = 'confidential');
ALTER VOLUME flowx.staging.uc_3 SET TAGS ('use_case' = 'uc3', 'data_classification' = 'confidential');

-- Table and column tags are better set through the spec; this is the shape the engine emits.
ALTER TABLE flowx.bronze.sgsn_cdr_raw SET TAGS ('use_case' = 'uc7', 'pii' = 'true');
ALTER TABLE flowx.bronze.sgsn_cdr_raw ALTER COLUMN msisdn SET TAGS ('pii' = 'true');
```

#### C.2.6 The information_schema catalog-qualification trap

This caused a real and long-lived misdiagnosis, so it is recorded explicitly.

An unqualified `information_schema` query resolves against `current_catalog()`. Querying it while
positioned in a different catalog returns **zero rows, truthfully and without error** — which reads
exactly like "no tags are applied" when in fact the tags exist and the query is looking in the
wrong place.

```sql
-- WRONG: resolves against current_catalog(), whatever that happens to be.
SELECT * FROM information_schema.table_tags WHERE schema_name = 'bronze';

-- RIGHT: always qualify with the catalog.
SELECT * FROM flowx.information_schema.table_tags  WHERE schema_name = 'bronze';
SELECT * FROM flowx.information_schema.column_tags WHERE schema_name = 'bronze';
SELECT * FROM flowx.information_schema.catalog_tags;
SELECT * FROM flowx.information_schema.schema_tags;
SELECT * FROM flowx.information_schema.volume_tags;
```

There is **no `SHOW TAGS` statement** in Unity Catalog. `information_schema` is the only way to read
tags back, which makes the qualification rule above the only way to verify anything in this clause.

#### C.2.7 Tagging constraints

| Constraint | Detail |
|---|---|
| Key length | 255 characters |
| Value length | 1000 characters |
| Tags per securable | 20. UC3's `customer_id` already carries 7 column tags — headroom is finite. |
| Unsupported securables | Secret scopes, jobs and pipelines cannot be tagged. |
| Propagation | **Not automatic.** A schema tag does not reach its tables. |
| Privilege | `ASSIGN` is required on the securable. |
| Governed tag policies | Fail at the `apply_governance` task, **not** at spec validation. A policy violation surfaces late, during a job run. |
| Idempotency | Tag DDL is naturally idempotent; re-running is safe. |

### 3 Naming standards

This clause is a reconciliation, not an aspiration. The repository has real conventions already in
use, and a standards document that contradicts them would simply be ignored.

#### C.3.1 Approach

The naming standards clause 1.2 in [document 03](03_platform_best_practice_and_naming_standards.md) records what the repository actually does today and treats it as authoritative.
Clause 1.3 in [document 03](03_platform_best_practice_and_naming_standards.md) proposes a forward-looking standard for **new** assets only. Clause 1.4 in [document 03](03_platform_best_practice_and_naming_standards.md) explains why
nothing existing should be renamed.

#### C.3.2 Authoritative in-place conventions

| Asset type | Convention | Example |
|---|---|---|
| Job and pipeline files | `<NNN>_<lfj\|ldp>_<uc>_<descriptor>` — number first, all lowercase | `007_lfj_uc6_ea_flood_warning` |
| Dataflow group | `dfg_` prefix | `dfg_uc6_ea_flood_warning` |
| Dataflow | `df_` prefix | `df_uc6_css_account` |
| Reconciliation flow | `rf_` prefix | `rf_uc3_excalibur` |
| Transformation step | `ts_` prefix | `ts_uc6_conform_address` |
| Volume | `uc_<N>` with an underscore | `uc_3`, `uc_6`, `uc_7` |
| Schema-config folder | `_schemas/<source>/` | `_schemas/css_account/` |
| Extraction target | `_extracted/<source>/` | `_extracted/ea_request/` |

**A worked precedent.** The brief that produced UC6 proposed `007_uc6_lfj_EA`. This was
**explicitly rejected** in favour of `007_lfj_uc6_ea_flood_warning`, which follows the repository's
number-first, lowercase convention. That rejection is the precedent: the repository convention wins
over a proposed one.

#### C.3.3 Live sequence-number registry

| Number | Assigned to |
|---|---|
| 001 | UC7 CDR ASN.1 |
| 002 | *(unassigned)* |
| 003–006 | UC3 |
| 007–008 | UC6 |
| **009** | **Next free** |

This registry currently lives only in prose, which is gap G-06.

#### C.3.4 Three table-naming conventions coexist in `flowx.bronze`

An honest finding rather than a recommendation:

| Convention | Example | Used by |
|---|---|---|
| Prefixed with the use case | `uc6_css_account` | UC6 |
| Descriptive with a type suffix | `emsc_cdr_raw` | UC7 |
| Bare entity name | `customer` | UC3 |

Because all three share one schema, only the first makes a collision structurally impossible. This
is gap G-09.

#### C.3.5 Forward-looking standard for new assets

For **new** assets only, adopt UC6's pattern: **prefix every Bronze table with `uc<N>_`.** It is the
only one of the three conventions under which two use cases cannot collide on a shared schema, and
it is already in production use, so it needs no new tooling or validation.

#### C.3.6 Migration note — rename nothing

**No renaming of existing assets is recommended.** The reasons are concrete, not cautious:

- **Renaming a target orphans its checkpoint** and forces a full refresh. For UC7's SGSN table, that
  is 175,048 records re-decoded; for a streaming CDC target, it is a rebuild of the entire history.
- **Renaming a job or pipeline breaks `${resources.jobs.*.id}` references** across the bundle, which
  fail at deploy time and, worse, can resolve to the wrong resource if a name is reused.

The migration is therefore **convention-forward only**: new assets follow C.3.5; existing assets
keep the names they have. The inconsistency in C.3.4 is documented rather than fixed.

### 4 Data sensitivity and access control

#### C.4.1 Classification

| Use case | Classification | Basis |
|---|---|---|
| **UC7** | **RESTRICTED** | Call detail records carrying MSISDN, IMSI, IMEI and cell-site location. This is communications metadata — location history and calling patterns — and is the most sensitive data in the estate. |
| **UC6** | **CONFIDENTIAL** | GPG-encrypted customer data which is, by its purpose, a register of vulnerable people at flood risk. Sensitive by inference as much as by content. |
| **UC3** | **CONFIDENTIAL**, with **RESTRICTED** columns | Telecoms reference data containing credentials and a credit-card column. |

#### C.4.2 A finding on UC3's credential columns that needs closing out

Four UC3 columns — `acc_password`, `blacklist_password`, `imei_black_list_pass` and
`gur_cr_card_no` — are tagged `NULL_AT_SOURCE`, and the streaming spec's
`data_standardization_sql` does `CAST(NULL AS STRING)` on them. On the streaming path, the sensitive
values therefore never land.

**This was observed on the streaming flows only.** The batch reconciliation path has not been
verified to do the same. If it does not, the batch path lands credentials and a card number that the
streaming path deliberately discards — and the reconciliation between the two would compare a
nulled column against a populated one.

**Action: verify the batch path before rollout.** This is listed in the appendix as an open item.

#### C.4.3 Column masks and row filters

None exist today (gap G-11). The shapes below are what would be implemented if critical finding 2 is
decided in favour of enforcement.

```sql
-- Mask a subscriber identifier: full value for the entitled group, last four digits otherwise.
CREATE OR REPLACE FUNCTION flowx.config.mask_msisdn(value STRING)
RETURN CASE
         WHEN is_account_group_member('uc7_restricted_readers') THEN value
         ELSE CONCAT('*******', RIGHT(value, 4))
       END;

ALTER TABLE flowx.bronze.sgsn_cdr_raw
  ALTER COLUMN msisdn SET MASK flowx.config.mask_msisdn;

-- Row filter restricting a Gold view to the caller's own region.
CREATE OR REPLACE FUNCTION flowx.config.filter_region(region STRING)
RETURN is_account_group_member('flood_warning_all_regions')
       OR region = current_user_region();

ALTER TABLE flowx.gold.flood_warning_summary
  SET ROW FILTER flowx.config.filter_region ON (region);
```

**Two sequencing rules apply:**

1. **Masks and filters are Unity Catalog DDL and must run *after* the pipeline update**, exactly as
   tag DDL does. Applying them before the table exists fails; applying them inside the pipeline is
   not possible.
2. **Take care masking a Bronze column that a Silver flow depends on.** A mask applies to the
   pipeline's own reads as well as to a human's. Masking a Bronze join key can silently change what
   the downstream transformation computes. Mask at the consumption layer in preference to Bronze.

#### C.4.4 Grants

```sql
-- Per-use-case reader groups, granted at table granularity.
GRANT USE CATALOG ON CATALOG flowx TO `uc7_restricted_readers`;
GRANT USE SCHEMA  ON SCHEMA flowx.bronze TO `uc7_restricted_readers`;
GRANT SELECT ON TABLE flowx.bronze.emsc_cdr_raw TO `uc7_restricted_readers`;
GRANT SELECT ON TABLE flowx.bronze.psgw_cdr_raw TO `uc7_restricted_readers`;
GRANT SELECT ON TABLE flowx.bronze.sgsn_cdr_raw TO `uc7_restricted_readers`;
GRANT SELECT ON TABLE flowx.bronze.tap310_raw   TO `uc7_restricted_readers`;

-- UC6 consumers read the conformed layers, not Bronze.
GRANT USE SCHEMA ON SCHEMA flowx.gold TO `flood_warning_analysts`;
GRANT SELECT ON TABLE flowx.gold.flood_warning_summary TO `flood_warning_analysts`;

-- Engineering needs the landing volume for UC7 operations.
GRANT READ VOLUME ON VOLUME flowx.landing.uc_7 TO `flowx_engineering`;
```

**Two grants that must NOT be made.** Both are easy mistakes with real consequences:

1. **Never `GRANT SELECT ON SCHEMA flowx.bronze` to any consumer group.** That schema mixes all
   three use cases. A grant intended to give a team its own Bronze tables gives it UC7's call detail
   records as well. Grant at table granularity in this schema, always.
2. **Never grant `READ VOLUME` on `uc_6` broadly.** The `output/` subtree holds egress files
   destined for external parties, and **volume grants are not path-scoped** — there is no way to
   grant `raw/` without also granting `output/`. A grant meant to let someone inspect inputs also
   hands them the outbound files.

### 5 Governance gaps

Fourteen gaps, with evidence and severity. The two HIGH items are promoted to the critical findings
at the front of this document.

| ID | Severity | Gap | Evidence | Recommended action |
|---|---|---|---|---|
| **G-01** | MEDIUM | No container-level tags. The catalog, schemas and volumes carry no tags; the framework tags tables only. | No `ALTER CATALOG`/`ALTER SCHEMA`/`ALTER VOLUME` tag DDL exists. | Apply the DDL in C.2.5 as a one-off, then add it to the runbook. |
| **G-02** | LOW | Orphaned `flowx.uc3_bronze` schema, inert and undroppable in place. | Schema exists; no flow targets it. | Confirm it is empty, then drop it in a maintenance window. |
| **G-03** | **HIGH** | **UC7 entirely ungoverned.** Four flows declare `governance_tags: {}` and the job has no `apply_governance` task. | `BT_Usecase/UC7/onboarding/UC7_cdr_asn_bronze.json` lines 51, 91, 136, 181; `uc7_cdr_asn_job.yml` task list. | Populate tags; add `apply_governance_uc7`. **Critical finding 1.** |
| **G-04** | **HIGH** | **Sink tags silently not applied.** Four UC6 sinks declare full `table_tags`; the engine emits only `ALTER TABLE` and iterates only ingestion and transformation rows. | `governance/tags.py` line 181. | Extend the engine, or reject sink tags at validation. **Critical finding 3.** |
| **G-05** | MEDIUM | Pending catalog migration, plus `resources/uc7/*.yml` hardcoding `catalog: flowx` instead of `${var.catalog}`. | `uc7_cdr_asn_job.yml` lines 35, 45, 62; `uc7_cdr_asn_pipeline.yml` lines 20, 37. | Replace the hardcoded values with the variable before the migration, or UC7 breaks on the renamed catalog. |
| **G-06** | LOW | The sequence-number registry lives only in prose. | C.3.3 is its only home. | Move it into a checked-in file, or a validation rule. |
| **G-07** | MEDIUM | UC3 carries unresolved `"?"` classification values. | UC3 spec tag values. | Resolve each to a real value from the C.2.4 domain. |
| **G-08** | MEDIUM | Landing volumes are undeclared manual prerequisites. | Only six volumes are bundle-managed; `uc_3`, `uc_6`, `uc_7` are not among them. | **Addressed by the setup notebook in [document 02](02_environment_deployment_and_setup.md).** Close once the notebook is adopted. |
| **G-09** | MEDIUM | Three table-naming conventions coexist in one shared Bronze schema. | C.3.4. | Adopt C.3.5 for new assets; do not rename existing ones. |
| **G-10** | LOW | No `cost_centre`, `created_by` or `retention_policy` tags anywhere. | Absent from every spec. | Add to the C.2.4 vocabulary and populate on new assets. |
| **G-11** | **HIGH** | **No column masks or row filters exist anywhere.** Tags describe; nothing enforces. | No mask or filter DDL in the repository. | Decide explicitly. **Critical finding 2.** |
| **G-12** | MEDIUM | Environment separation is by tag, not by boundary. A `poc` tag does not prevent a production query. | `environment` tag is the only separator. | Separate catalogs per environment in the target state. |
| **G-13** | MEDIUM | One secret scope, no rotation record, and a single passphrase covering both UC6 ingress and egress. | `flowx.config.pgpkey` is used by both directions. | Split ingress and egress passphrases; record a rotation schedule. |
| **G-14** | MEDIUM | `SGSN.asn1` is referenced by the UC7 spec and validated by the setup notebook, but exists nowhere in the repository. `GGSN.asn1` and `TAP.311.asn1` are present but referenced by no spec. | `BT_Usecase/UC7/data/asn_schema/` holds `EMSC`, `GGSN`, `PSGW`, `TAP.310`, `TAP.311`; the spec names `EMSC`, `PSGW`, `SGSN`, `TAP.310`. | Confirm the customer supplies `SGSN.asn1` directly to the upload folder, and record that `GGSN`/`TAP.311` are generator inputs rather than pipeline schemas. See C.1.6. |

#### C.5.1 Priority sequence

1. **G-03** — tag UC7 and add its `apply_governance` task. It is the most sensitive data in the
   estate and the least governed.
2. **G-04** — stop the silent sink-tag no-op, by either of the two routes.
3. **G-11** — take and record the enforcement decision.
4. **G-05** — replace the hardcoded catalog values before the migration runs.
5. **G-01, G-07, G-08** — apply container tags, resolve the `"?"` values, adopt the setup notebook in [document 02](02_environment_deployment_and_setup.md).

### 6 Appendix — key source files

| File | Relevance |
|---|---|
| `src/flowx/lakeflow_framework/governance/tags.py` | Tag application; line 181 is the ingestion-and-transformation-only loop. |
| `src/flowx/lakeflow_framework/onboarding/spec_validator.py` | Line 297, `ALLOWED_GOVERNANCE_TAGS_KEYS`. |
| `src/flowx/lakeflow_framework/crypto/secrets.py` | Lines 81–86, the secret-scope fallback order. |
| `BT_Usecase/UC3/onboarding/uc3_excalibur_streaming_cdc.json` | UC3 streaming spec and its `NULL_AT_SOURCE` columns. |
| `BT_Usecase/UC3/onboarding/uc3_excalibur_batch_recon.json` | UC3 batch spec — the path needing the C.4.2 check. |
| `BT_Usecase/UC6/onboarding/uc6_ea_flood_warning.json` | UC6 spec, including the four sink flows of gap G-04. |
| `BT_Usecase/UC7/onboarding/UC7_cdr_asn_bronze.json` | UC7 spec; the four empty `governance_tags` blocks. |
| `resources/uc7/uc7_cdr_asn_job.yml` | UC7 job; the missing `apply_governance` task and hardcoded catalog. |
| `resources/uc3/uc3_streaming_cdc_job.yml` | The `apply_governance_uc3` task to model UC7's on. |
| `BT_Usecase/UC7/docs/UC7_CDR_ASN_Test_Report.md` | UC7 verification, including the SMSC and MMSC open items. |
| `docs/DATA_PROVENANCE_CLASSIFICATION.md` | Which source files are customer-provided and which are regenerable. |
| `docs/SCRIPTS_GUIDE.md` | Index of every script, including the three test-data generators. |

---

---

*End of document 01. See [04 — Consolidated rollout runbook](04_consolidated_rollout_runbook.md) for the ordered sequence that applies this governance.*
