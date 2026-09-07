# 03 — Databricks Naming and Best Practice Standards

**FlowX (NextGen Metadata Framework)**
**Framework release:** v1.7.4 · **Wheel artefact version:** 0.0.3
**Document date:** 7 September 2026
**Audience:** Platform Architects, Data Engineers, Data Governance, Release Management

---

## What this document is

The enterprise naming, structure and tagging standard for every Databricks object this platform
creates. It covers what an object is called, how it is qualified, and which Unity Catalog tags it
must carry.

It is scoped strictly to Databricks assets. Application interface concerns, release banners,
infrastructure configuration and use-case specifics are out of scope and belong to the documents
that own them.

The standard is a **reconciliation, not an aspiration.** The repository already has real conventions
in force, and a standards document that contradicts what engineers actually type would simply be
ignored. So: Clause 1 records what is authoritative today, Clause 2 defines the forward-looking
template for **new** assets, Clause 3 gives the per-object-type standards, Clause 4 the tag
vocabulary, and Clause 5 the build practice that protects it all.

**Catalog scope.** Every example in this document uses `br_digital_poc`. Wherever a path appears as
`/Volumes/<catalog>/...` the catalog is a **parameter, never a literal** — the `target_catalog`
widget in the setup notebook, `${var.catalog}` in a bundle, `{{catalog}}` in an onboarding spec.

---

## Clause 1 — Authoritative in-place conventions

These are in force today. Where a proposed convention conflicts with one of these, the repository
convention wins.

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

**A worked precedent.** The brief that produced UC6 proposed `007_uc6_lfj_EA`. This was explicitly
**rejected** in favour of `007_lfj_uc6_ea_flood_warning`, which follows the number-first, lowercase
convention above. That rejection is the precedent: a proposal does not override the convention.

### Live sequence-number registry

| Number | Assigned to |
|---|---|
| 001 | UC7 CDR ASN.1 |
| 002 | *(unassigned)* |
| 003–006 | UC3 |
| 007–008 | UC6 |
| **009** | **Next free** |

Take the next free number, then update this table in the same change. The registry lives only in
prose, so nothing enforces it — a duplicate number is caught by review or not at all.

---

## Clause 2 — The forward-looking naming template

For **new** assets, the general template is:

```text
<project>_<environment>_<layer|purpose>_<object_type>
```

Applied with the pragmatism the platform already shows: not every object type needs all four
segments, and segments that are already implied by the object's container are omitted rather than
repeated. A table in `br_digital_poc.bronze` does not repeat the catalog or the layer in its own
name — that would be `bronze.bronze_customer`. The rule is **qualify by container first, then name
the difference.**

| Segment | Values | Omit when |
|---|---|---|
| `<project>` | `flowx`, or the consuming use case `uc3` / `uc6` / `uc7` | Never omit on a shared schema |
| `<environment>` | `poc`, `dev`, `prod` | The catalog already carries it |
| `<layer\|purpose>` | `bronze`, `silver`, `gold`, `landing`, `staging`, `config`, `observability` | The schema already carries it |
| `<object_type>` | `lfj` (job), `ldp` (pipeline), `dfg`, `df`, `rf`, `ts`, `raw`, `vw` | The object's type is unambiguous from its container |

### Casing and character rules

| Rule | Detail |
|---|---|
| Case | **Lowercase throughout.** Unity Catalog identifiers are case-insensitive but case-preserving, so mixed case creates objects that look different and compare the same. |
| Separator | Underscore. Never a hyphen — a hyphen forces backtick-quoting in every SQL statement that touches the object. |
| Leading character | A letter. Never a digit and never an underscore. |
| Reserved | Do not use SQL keywords as bare identifiers (`order`, `user`, `table`). |
| Length | Keep under 60 characters. Long names get truncated in the UI and in event-log payloads. |

---

## Clause 3 — Object naming standards

### 3.1 Catalogs

| Item | Standard |
|---|---|
| Template | `<project>_<environment>` |
| This platform | `br_digital_poc` |
| Rule | One catalog per environment. Never mix environments inside one catalog — the catalog is the isolation boundary that grants, tags and lineage all key on. |
| Constraint | **A catalog cannot be declared in a bundle.** Unity Catalog Default Storage rejects `CREATE CATALOG` without a `MANAGED LOCATION`. The catalog is a manual prerequisite and remains one. |

### 3.2 Schemas and databases

Schemas carry the medallion layer. The layer belongs here, not in the table name.

| Schema | Holds |
|---|---|
| `landing` | Raw, unprocessed arrivals. Volumes live here. Classified **restricted** — this is where undecrypted and unmasked source data sits. |
| `staging` | Transient pre-Bronze work: decrypted, extracted or simulator output awaiting ingestion. |
| `bronze` | Ingested, typed, framework-stamped source-of-record tables. Schema-on-read resolved; no business logic applied. |
| `silver` | Conformed, deduplicated, joined. Business keys resolved. |
| `gold` | Aggregated and semantic. The layer consumers and dashboards read. |
| `config` | Control tables, onboarding-spec volumes, mask and filter functions. |
| `observability` | Event-log exports, run telemetry, app logs. |

**One structural warning.** `bronze` is currently shared by all three use cases, which is why the
`uc<N>_` table prefix in 3.3 is mandatory for new tables — on a shared schema it is the only thing
making a collision between two use cases structurally impossible.

### 3.3 Tables and views

| Object | Template | Example |
|---|---|---|
| Bronze table | `uc<N>_<entity>` | `uc6_css_account` |
| Bronze table, raw-decoded | `uc<N>_<entity>_raw` | `uc7_sgsn_cdr_raw` |
| Silver table | `uc<N>_<entity>` | `uc6_customer_address` |
| Gold / semantic table | `uc<N>_<subject>_<grain>` | `uc6_flood_warning_summary` |
| Staging table | `<entity>_<mode>` | `physical_device_batch`, `customer_stream` |
| Quarantine table | `<source_table>_quarantine` | `uc7_psgw_cdr_raw_quarantine` |
| View | `vw_<subject>` | `vw_uc6_active_warnings` |
| Materialized view | `mv_<subject>` | `mv_uc6_warning_daily` |
| Streaming table | No suffix — the type is a property, not a name | `uc6_css_account` |

**Do not encode the object's Delta type in its name.** A streaming table that is later rebuilt as a
materialized view would need renaming, and renaming is prohibited (3.8). `mv_` is the one exception,
retained because a materialized view's refresh semantics genuinely change how a consumer queries it.

**Three conventions coexist in Bronze today** — an honest finding, not a recommendation:

| Convention | Example | Used by |
|---|---|---|
| Prefixed with the use case | `uc6_css_account` | UC6 |
| Descriptive with a type suffix | `emsc_cdr_raw` | UC7 |
| Bare entity name | `customer` | UC3 |

Only the first makes a collision structurally impossible on a shared schema. New tables use the
first; existing tables are not renamed.

### 3.4 Jobs, pipelines and tasks

| Object | Template | Example |
|---|---|---|
| Job | `<NNN>_lfj_<uc>_<descriptor>` | `007_lfj_uc6_ea_flood_warning` |
| Pipeline | `<NNN>_ldp_<uc>_<descriptor>` | `001_ldp_uc7_cdr_asn_bronze` |
| Bundle resource key | Matches the file basename, no number prefix | `uc6_ea_flood_warning_job` |
| Task key | `<verb>_<object>` — imperative, lowercase | `setup_control_tables`, `onboard_uc6`, `run_pipeline_update`, `apply_governance_uc6`, `observability_export` |

**Task keys are a contract, not a label.** `depends_on` references them by string, so renaming a
task silently breaks the dependency graph of every task downstream of it.

**Every job that writes governed tables must carry an `apply_governance_<uc>` task.** Tag DDL is
Unity Catalog DDL and cannot run inside a pipeline update, so it is always a separate task ordered
after `run_pipeline_update`. A job without one produces correct data with no governance on it.

### 3.5 Unity Catalog identities — groups and service principals

Naming here is a security control. A group whose scope is not legible from its name gets granted by
mistake.

| Identity | Template | Example |
|---|---|---|
| Admin group | `<project>_<environment>_admin` | `br_digital_poc_admin` |
| Read group, platform-wide | `<project>_<environment>_read` | `br_digital_poc_read` |
| Write group | `<project>_<environment>_write` | `br_digital_poc_write` |
| Use-case reader | `<uc>_<classification>_readers` | `uc7_restricted_readers` |
| Consumer / business group | `<subject>_<role>` | `flood_warning_analysts` |
| Engineering group | `<project>_engineering` | `flowx_engineering` |
| Service principal | `sp_<project>_<environment>_<purpose>` | `sp_flowx_poc_deploy` |

| Rule | Detail |
|---|---|
| Source of truth | Groups are **account-level** and assigned to the workspace. Never create a workspace-local group — it cannot hold a Unity Catalog grant. |
| Grant target | Grant to a group. **Never grant to a named user**, and never to a service principal directly — put the principal in a group. |
| Least privilege | A service principal gets exactly the privileges its job needs. A deploy principal does not need `SELECT` on Bronze. |
| Ownership | Every securable has an explicit owner group. An object owned by a departed individual is unmanageable. |

### 3.6 Genie spaces

| Item | Standard |
|---|---|
| Template | `<project> <environment> — <business subject>` — title case, spaces allowed; Genie spaces are read by business users, not typed in SQL |
| Example | `BR Digital POC — Flood Warning` |
| Scope | One space per business subject. Never one space over the whole catalog: Genie's answer quality falls off sharply as the table count rises. |
| Binding | Point a space at **Gold / semantic** tables only. A space bound to Bronze exposes unconformed columns and pre-masking values to natural-language query. |
| Governance | A Genie space runs as the querying user, so Unity Catalog grants apply — but only if the underlying grants are correct. A space is not an access-control layer. |

### 3.7 AI/BI dashboards (Lakeview)

| Item | Standard |
|---|---|
| Display name | `<project> <environment> — <subject>` | 
| Example | `BR Digital POC — Control Metadata` |
| File | `<subject>.lvdash.json`, lowercase with underscores |
| Resource key | `<subject>_dashboard` |
| Query rule | Dashboard queries keep **bare table names**. The catalog and schema are injected per target by the `dataset_catalog` / `dataset_schema` parameters, so one dashboard file deploys to every environment unedited. Hardcoding a three-part name breaks that. |
| Binding | Bind to Gold / semantic tables and to the control and observability tables. Never to Bronze. |

### 3.8 Renaming — the standing prohibition

**No existing asset is renamed to comply with this document.** The reasons are concrete:

- **Renaming a target orphans its checkpoint** and forces a full refresh. For UC7's SGSN table that
  is 175,048 records re-decoded; for a streaming CDC target it is a rebuild of the entire history.
- **Renaming a job or pipeline breaks `${resources.jobs.*.id}` references** across the bundle. These
  fail at deploy time and — worse — can resolve to the *wrong* resource if a name is later reused.
- **Renaming a streaming source breaks the next incremental update**, not the current one. The
  failure is deferred, which makes it expensive to diagnose.

The migration is therefore **convention-forward only**: new assets follow this standard, existing
assets keep the names they have, and the inconsistency in 3.3 is documented rather than fixed.

---

## Clause 4 — Unity Catalog tags and metadata keys

### 4.1 Canonical keys

These are the only tag keys in the vocabulary. A key not on this list is not applied.

| Key | Values | Applies to | Meaning |
|---|---|---|---|
| `environment` | `poc` · `dev` · `prod` | Catalog, schema, volume | Deployment stage |
| `data_classification` | `public` · `internal` · `confidential` · `restricted` | Schema, volume, table, column | Sensitivity. Drives who may be granted. |
| `owner` | An account group name | Catalog, schema, table | The group accountable for the object |
| `cost_center` | Finance code | Catalog, schema | Chargeback attribution |
| `layer` | `landing` · `bronze` · `silver` · `gold` | Schema, table | Medallion position |
| `use_case` | `uc3` · `uc6` · `uc7` | Volume, table | Owning use case. **Mandatory on every table in a shared schema.** |
| `pii` | `true` · `false` | Table, column | Contains personally identifiable data |

Values are lowercase and drawn only from the domain listed. `data_classification` in particular is
graded — `restricted` is strictly more sensitive than `confidential` — so a free-text value silently
breaks any policy built on ordering.

### 4.2 How tags are applied

| Level | Route |
|---|---|
| Table, column | **The `governance_tags` block in the onboarding spec.** This is the preferred route: it is reproducible, it is version-controlled, and it re-applies on every run. |
| Catalog, schema, volume | Manual DDL — the framework tags tables and columns only. Consolidated in [`setup_scripts/01_container_tags.sql`](setup_scripts/01_container_tags.sql). |

### 4.3 Constraints worth knowing before you design a vocabulary

| Constraint | Detail |
|---|---|
| Key length | 255 characters |
| Value length | 1000 characters |
| Tags per securable | **20.** UC3's `customer_id` already carries 7 column tags — headroom is finite, so do not spend it on tags nothing reads. |
| Unsupported securables | Secret scopes, jobs and pipelines **cannot be tagged**. Do not design a cost-attribution scheme that assumes job tags. |
| Propagation | **Not automatic.** A schema tag does not reach its tables. Table-level tags are additional, never redundant. |
| Privilege | `ASSIGN` on the securable. |
| Governed tag policies | Fail at the `apply_governance` task, **not** at spec validation — a policy violation surfaces late, during a job run. |
| Reading tags back | There is **no `SHOW TAGS`**. `information_schema` is the only route, and it must always be catalog-qualified. |

### 4.4 Enforcement is not classification

Tags are applied but never **enforced**. No column mask and no row filter exists in the estate
today, so classification is descriptive metadata: it records that a column is restricted, and
restricts nothing. Where enforcement is adopted, the shapes are in
[`setup_scripts/02_column_masks_and_row_filters.sql`](setup_scripts/02_column_masks_and_row_filters.sql),
which is deliberately marked not-applied.

---

## Clause 5 — Build and deployment practice

Not general advice. These are failure modes this repository has actually hit.

- **Run `npm run build` in `databricks-app/web` after touching `web/src`.** Databricks Apps does
  **not** build at deploy time. An un-rebuilt `web/dist/` keeps serving the previous bundle, so the
  change simply does not appear and the deployment still looks successful.
- **`bundle deploy` alone does not update the application.** `bundle run` is required. A deploy that
  reports success while the app serves old code is the single most common false positive here.
- **Run `node --check` on any file edited under `web/src`.** This repository has twice had a broad
  regular-expression edit damage source — once swallowing roughly 650 lines of `Builder.jsx`. Anchor
  edits on exact line prefixes; never bulk-edit with a broad pattern.
- **Never deploy while a pipeline or test wave is running.** `bundle deploy` prunes superseded
  artefacts from `<artifact_path>/.internal/`, so a deploy issued mid-update kills the run with
  `ENVIRONMENT_PIP_INSTALL_ERROR`. Unique per-deploy wheel filenames prevent overwrite-in-place, not
  removal.
- **Escape braces inside the DDL f-strings in `control_plane/ddl_definitions.py` as `{{ }}`.** An
  unescaped `{...}` in a column `COMMENT` is evaluated as an expression and breaks every
  control-table DDL.
- **Validate a spec against both gates.** The JSON schema and `spec_validator.py` drift, and only
  the schema rejects unknown keys. Passing one is not passing the other.

---

## The delivery set

| # | Document | Covers |
|---|---|---|
| 01 | [Use case asset inventory and governance](01_usecase_asset_inventory_and_governance.md) | Asset register by layer, DAG lineage, tagging, sensitivity, access control |
| 02 | [Environment deployment and setup](02_environment_deployment_and_setup.md) | The setup and staging notebook, its design, and how to run it |
| 03 | Databricks naming and best practice standards | *This document* |
| 04 | [Consolidated rollout runbook](04_consolidated_rollout_runbook.md) | The ordered end-to-end sequence |

Setup DDL referenced above lives in [`setup_scripts/`](setup_scripts/).

---

*End of document 03.*
