# 01 — Use Case Asset Inventory and Governance

**FlowX (NextGen Metadata Framework) · UC3, UC6, UC7**
**Framework release:** v1.7.4 · **Wheel artefact version:** 0.0.3
**Document date:** 7 September 2026
**Audience:** Data Governance Lead, Information Security, Data Platform Architects

---

## What this document is

The complete register of every Databricks asset the three use cases own — sources, jobs, pipelines,
tables by medallion layer, dashboards, volumes, secrets and control tables — together with each
table's role in the orchestrating DAG, how it is tagged, how sensitive it is and who may read it.

It describes the estate **as it actually stands**, not a target state. Where an object is a manual
prerequisite rather than a bundle-declared resource, it says so, because that distinction determines
whether a fresh deployment will create it.

All DDL referenced here lives in [`setup_scripts/`](setup_scripts/), not in this narrative.

**Catalog.** Every asset is written as `br_digital_poc.<schema>.<object>`. The catalog is always a
**parameter, never a literal** — the `target_catalog` widget in the setup notebook, `${var.catalog}`
in a bundle, `{{catalog}}` in an onboarding spec.

## The three use cases in one line each

| Use case | One-line description | Sensitivity |
|---|---|---|
| **UC3** | Excalibur telecoms reference data — three tables (`physical_device`, `customer`, `subscriber`) ingested as a streaming CDC feed and reconciled by a parallel batch path that heals gaps back into the stream. | CONFIDENTIAL, with RESTRICTED columns (credentials, a card number) |
| **UC6** | Environment Agency flood warning — six GPG-encrypted or gzipped customer feeds landed, decrypted, conformed through Silver to Gold, and egressed as four files. | CONFIDENTIAL — inherently a vulnerable-person register |
| **UC7** | Call detail records — four network elements (EMSC, PSGW, SGSN, TAP) decoded from BER-encoded ASN.1 into Bronze, with per-table quarantine. | RESTRICTED — MSISDN, IMSI, IMEI, cell-site location |

---

## Part 0 — Framing: what can and cannot be automated

Four constraints shape everything below. Stating them first prevents the register being read as a
list of things `bundle deploy` creates, which it is not.

1. **The catalog cannot be declared in a bundle.** Unity Catalog Default Storage rejects
   `CREATE CATALOG` without a `MANAGED LOCATION`. This was attempted and reverted on 2 September
   2026. The catalog is, and remains, a manual prerequisite.
2. **Only six volumes are bundle-managed.** The wheels volume, the onboarding-specs volume, and four
   `flowx_sample` volumes. **Every use-case volume — `uc_3`, `uc_6`, `uc_7` — is a manual
   prerequisite**, created by the setup notebook in [document 02](02_environment_deployment_and_setup.md).
3. **The nine control tables are created at runtime**, by the `setup_control_tables` job task — not
   by `bundle deploy`. A successful deploy leaves you with no control tables; they appear on the
   first job run. This is the most common "the tables are missing after a successful deploy" report.
4. **Tags are applied but never enforced.** No column mask and no row filter exists anywhere in the
   estate. Classification is descriptive metadata; it restricts nothing.

Provenance markers used throughout: **PRE-EXISTING** (already in the workspace, reused),
**Bundle-managed** (created by `bundle deploy`), **Pipeline** (created by a pipeline update),
**MANUAL** (an operator prerequisite).

---

## Part 1 — Platform-wide assets

### 1.1 Catalog and schemas

| Asset | Type | Provenance | Notes |
|---|---|---|---|
| `br_digital_poc` | Catalog | **MANUAL** | Cannot be declared in a bundle. |
| `br_digital_poc.config` | Schema | Bundle-managed | Control tables, wheels and spec volumes. Carries a `lifecycle` guard. |
| `br_digital_poc.landing` | Schema | Pipeline | UC7 raw arrivals. Not a declared bundle resource. |
| `br_digital_poc.staging` | Schema | Pipeline | UC3 and UC6 volumes and staging tables. Not a declared bundle resource. |
| `br_digital_poc.bronze` | Schema | PRE-EXISTING | **Shared by all three use cases** — see the grant caution in 6.3. |
| `br_digital_poc.silver` | Schema | Pipeline | UC6 conformed layer. |
| `br_digital_poc.gold` | Schema | Pipeline | UC6 aggregated layer and egress sinks. |
| `br_digital_poc.observability` | Schema | Pipeline | Observability exports. |
| `br_digital_poc.flowx_sample` | Schema | Bundle-managed | Sample and bootstrap assets. |

`bronze`, `silver`, `gold`, `staging`, `landing` and `observability` are **not** declared as bundle
resources — they are created implicitly by the pipeline or assumed to exist.

### 1.2 Volumes, secrets, app and shared jobs

| Asset | Type | Provenance | Notes |
|---|---|---|---|
| `br_digital_poc.config.wheels` | Volume | Bundle-managed | Framework wheel, version-scoped: `0.0.3/`. The bundle artifact path. |
| `br_digital_poc.config.onboarding_specs` | Volume | Bundle-managed | Specs uploaded by the application. |
| `br_digital_poc.observability.app_logs` | Volume | Pipeline | Subfolders per use case. |
| `br_digital_poc.config.pgpkey` | Secret | **MANUAL** | GPG passphrase. Covers **both** UC6 ingress and egress. |
| FlowX onboarding app | Databricks App | Bundle-managed | Spec builder front end (`flowx-onboarding`). Bound to `onboarding_job` and the specs volume. |
| `onboarding_job` | Job | Bundle-managed | Generic onboarding job. **Every use-case job delegates to it** via `run_job_task`. |
| `framework_config_onboarding_job` | Job | Bundle-managed | Bulk onboarding across all specs. |
| Preflight function | UC function | Bundle-managed | Pre-run spec validation UDF. |

### 1.3 The nine control tables

All nine live in `br_digital_poc.config`, are created `IF NOT EXISTS` by the `setup_control_tables`
task, and are defined in `src/flowx/lakeflow_framework/control_plane/ddl_definitions.py`.

| # | Control table | Holds | Read by the dashboard? |
|---|---|---|---|
| 1 | `dataflow_group_spec` | Group identity and activation state | Yes |
| 2 | `ingestion_flow_spec` | Source-to-Bronze flow rows, including `governance_tags_json` | Yes |
| 3 | `transformation_flow_spec` | Bronze-onward flow rows, including `governance_tags_json` | Yes |
| 4 | `onboarding_audit_log` | Onboarding history | Yes |
| 5 | `reconciliation_flow_spec` | Reconciliation flow rows | Yes |
| 6 | `reconciliation_run_log` | Reconciliation execution history | Yes |
| 7 | `reconciliation_mismatch_log` | Per-row mismatch detail | No |
| 8 | `reconciliation_result` | Reconciliation outcomes | No |
| 9 | `observability_config` | Observability destinations | No |

> Only tables 2 and 3 carry `governance_tags_json` that the `apply_governance` task acts on.
> Reconciliation flows and sinks are not tagged.

**Deactivation caveat.** Deleting a flow from a spec does **not** deactivate its control row. The row
persists and keeps driving the DAG.

---

## Part 2 — UC3: Excalibur telecoms reference data

### 2.1 Sources

| Source | Read as | Path / table | Format |
|---|---|---|---|
| Excalibur change stream | `zerobus` ingestion flow, one physical read shared by 3 flows | `br_digital_poc.staging.oracle_excalibur_cdc` | Delta, multiplexed Debezium envelopes |
| Excalibur batch snapshots (x3) | **transformation flow**, `source_inputs[].is_streaming: false` | `br_digital_poc.oracle_excalibur_batch.{physical_device,customer,subscriber}` | Delta, written by the Lakeflow Connect Oracle query-based connector |
| *Historical:* Excalibur batch extracts (x3) | `autoloader` | `/Volumes/br_digital_poc/staging/uc_3/batch/<table>/batch_date=<YYYY-MM-DD>/` | CSV, header, comma. **Superseded v0.0.7.** |
| *Historical:* simulated change stream (x3) | `zerobus` | `br_digital_poc.staging.<table>_stream` | Delta. **Superseded v0.0.4.** |

`staging.oracle_excalibur_cdc` is both the streaming source and the **heal target** of all three
reconciliation flows, so a healed row is applied by the same CDC engine as any real change.

**The batch snapshots cannot be read by an ingestion flow, at all.** The Lakeflow Connect tables are
MERGE-written (`scd_type SCD_TYPE_1`), Delta refuses to stream a MERGE-written table
(`DELTA_SOURCE_TABLE_IGNORE_CHANGES`) and `skipChangeCommits` is refused framework-wide; on top of
that, `engine/source_plane.py` plans every ingestion source request `want_stream=True`
unconditionally, with no batch ingestion reader to fall back to. A transformation flow's
`is_streaming: false` is honoured verbatim and gives a real `spark.read.table(...)`, which is why
the three batch flows are transformation flows.

### 2.2 Jobs and pipelines

| # | Job | Pipeline triggered | Tasks in order |
|---|---|---|---|
| 1 | `003_lfj_uc3_excalibur_streaming_simulator` | *(none)* | `delta_table_setup` → `stream_producer` |
| 1b | `003b_lfj_uc3_excalibur_seed` | *(none)* | `setup_control_tables` → `onboard_streaming_cdc` → `onboard_batch_recon` (once per workspace, and after any spec change) |
| 2 | `004_lfj_uc3_excalibur_streaming_cdc` | `004_ldp_uc3_excalibur_streaming_cdc` | `run_pipeline_update` → `observability_export` |
| 3 | `005_lfj_uc3_excalibur_batch_recon` | `006_ldp_uc3_excalibur_batch_recon` | `run_pipeline_update` → `heal_physical_device` ‖ `heal_customer` ‖ `heal_subscriber` → `observability_export` |
| 3b | `009_lfj_uc3_excalibur_governance` | *(none)* | `tag_streaming_cdc` → `tag_batch_recon` (once the tables exist, and after any `governance_tags` change) |

**Ordering is a hard constraint.** Job 3 must run after Job 2, and the three jobs must **not** be
parallelised — Job 3 reads Job 2's bronze tables, and concurrent onboarding into one catalog
risks a Unity Catalog conflict (the seed job's two onboarding tasks are serial for that reason).

Dataflow groups: `dfg_uc3_excalibur_streaming_cdc`, `dfg_uc3_excalibur_batch_recon`.

**The three `heal_*` tasks in job 3 are not optional.** Its reconciliation flows are
`execution_mode: "pipeline_audit_only"`, which registers the comparison inside the pipeline update
but **no heal lane at all**. Healing is therefore one `notebooks/05_reconciliation/05_reconciliation_engine.py`
task per `reconciliation_id`. Without them the batch lane compares and reports forever while never
correcting anything, with every run green.

### 2.3 Tables by layer, and their DAG role

#### Staging

| Table | Written by | Upstream | DAG role |
|---|---|---|---|
| `staging.oracle_excalibur_cdc` | The Debezium/Zerobus CDC connector; **also the heal append target** of all three UC3 reconciliation flows | Excalibur CDC feed + recon heal | **The multiplexed CDC landing table and re-entry point.** One physical read feeds all three streaming flows; healed rows are appended as complete Debezium envelope rows and applied by the same CDC engine. |
| `oracle_excalibur_batch.physical_device` | **Lakeflow Connect Oracle query-based connector** (outside this framework) | Excalibur Oracle | **Batch source.** MERGE-written, `scd_type SCD_TYPE_1`, so one row per PK. Cannot be streamed. |
| `oracle_excalibur_batch.customer` | as above | as above | as above |
| `oracle_excalibur_batch.subscriber` | as above | as above | as above |
| `staging.physical_device_batch` | `df_uc3_physical_device_batch_load` (a **transformation flow**, `is_streaming: false`) | `oracle_excalibur_batch.physical_device` | **Upstream input to reconciliation.** Materialized view, `TRUNCATE_AND_LOAD`, full snapshot recomputed per update. No partition column. |
| `staging.customer_batch` | `df_uc3_customer_batch_load` | `oracle_excalibur_batch.customer` | as above |
| `staging.subscriber_batch` | `df_uc3_subscriber_batch_load` | `oracle_excalibur_batch.subscriber` | as above |
| `staging.{physical_device,customer,subscriber}_stream` | Job 1 simulator | Landing CSV `uc_3/streaming` | **Historical.** The simulator's output, and the heal target until v0.0.7. Nothing reads them now. |

Job 1 also creates a producer cursor table. Job 1 and the `uc_3` volume are historical: the
streaming lane moved to the real CDC feed in v0.0.4 and the batch lane to Lakeflow Connect in
v0.0.7.

#### Bronze

| Table | Written by | Upstream | CDC | DAG role |
|---|---|---|---|---|
| `bronze.physical_device` | `df_uc3_physical_device_stream_cdc` | `staging.physical_device_stream` | **SCD1**, PK `customer_id, subscriber_no, equipment_no, phy_seq_no` | **Target consumer** of the stream lane; **reconciliation target** for the batch lane. |
| `bronze.customer` | `df_uc3_customer_stream_cdc` | `staging.customer_stream` | **SCD2**, PK `customer_id` | as above |
| `bronze.subscriber` | `df_uc3_subscriber_stream_cdc` | `staging.subscriber_stream` | **SCD1**, PK `subscriber_no, customer_id` | as above |

All three generate framework hash columns and treat `src_deleted_flg = "1"` as a delete.

#### Reconciliation-derived datasets

Three flows, each comparing a batch staging table against its Bronze CDC target and appending the
miss set back into the CDC landing table as Debezium envelope rows. All three are
`execution_mode: "pipeline_audit_only"`, so the comparison runs inside the pipeline update and the
append runs in the job's `heal_*` tasks.

| Reconciliation flow | Source | Target | Match keys | Heals into |
|---|---|---|---|---|
| `rf_uc3_physical_device_batch_vs_bronze` | `staging.physical_device_batch` | `bronze.physical_device` | 4 keys, 25 compare columns | `staging.oracle_excalibur_cdc` |
| `rf_uc3_customer_batch_vs_bronze` | `staging.customer_batch` | `bronze.customer`, filtered to `__END_AT IS NULL` | `customer_id`, ~89 compare columns | `staging.oracle_excalibur_cdc` |
| `rf_uc3_subscriber_batch_vs_bronze` | `staging.subscriber_batch` | `bronze.subscriber` | 2 keys, ~130 compare columns | `staging.oracle_excalibur_cdc` |

`bronze.customer` is SCD2 and holds every historical version, so its target side carries
`filter_condition: "__END_AT IS NULL"`, because the matcher collapses duplicate keys
`MATCHED > VALUE_DRIFT > MISSING`, so without it a stale closed version could mask real drift on
the current row. The SCD1 tables hold one row per key and need no filter.

Each flow's `transform_sql` reshapes the miss set into a complete Debezium envelope row (the 9
landing columns, the verbatim Kafka-Connect `schema` block, UPPERCASE Oracle field names, epoch
millis on Connect `Timestamp` fields, `before: null`, `op: 'r'`, and an SCN one past the current
maximum) so the healed row is applied by the same CDC engine as any real change.

Each publishes `recon__<rid>__<tid>__metrics` into `reconciliation`; the `_`-prefixed prepare,
classify and missing datasets are pipeline-internal. `mismatch_log_capture` is off, so the mismatch
dataset is suppressed. There is **no** pulse or heal-sink dataset: audit-only does not register an
L5 lane.

**The UC3 DAG in one line:** the Debezium feed lands in `staging.oracle_excalibur_cdc` → Job 2
CDC-applies it into `bronze.*` → Job 3 recomputes `staging.*_batch` from the Lakeflow Connect Oracle
tables, reconciles batch against Bronze, and its `heal_*` tasks append the miss set back into
`staging.oracle_excalibur_cdc` as envelope rows, which Job 2's continuous pipeline then applies.

### 2.4 Other UC3 assets

| Asset | Type | Provenance | Notes |
|---|---|---|---|
| `br_digital_poc.staging.uc_3` | Volume | **MANUAL** | Created by the setup notebook. **Historical**: neither lane reads it since v0.0.7. |
| `uc_3/_schemas/<table>_batch/` | Path | Notebook | **Historical.** Auto Loader schema checkpoints. Job 3 no longer uses Auto Loader. |
| `br_digital_poc.oracle_excalibur_batch` | Schema | **Lakeflow Connect** | Holds the three Oracle query-based-connector snapshot tables Job 3 reads. Not a declared bundle resource, and not written by this framework. |
| `br_digital_poc.uc3_bronze` | Schema | PRE-EXISTING | **Orphaned and inert.** Nothing writes to it. |

---

## Part 3 — UC6: Environment Agency flood warning

### 3.1 Sources

| Source feed | Landing path | File pattern | Format |
|---|---|---|---|
| `ea_request` | `staging/uc_6/raw/` → `_extracted/ea_request/` | `EE_*-REQUEST_*[Oo][Ff]*.csv.gz.gpg` | GPG-symmetric + gzip → CSV, pipe, **header** |
| `css_account` | `staging/uc_6/raw/` | `CSS_account_[0-9]*.dat.gz` | gzip CSV, pipe, headerless, 56 fields |
| `css_account_address` | `staging/uc_6/raw/` | `CSS_account_address_*.dat.gz` | gzip CSV, pipe, headerless, 19 fields |
| `css_subscription` | `staging/uc_6/raw/` | `CSS_subscription_*.dat.gz` | gzip CSV, pipe, headerless, 47 fields |
| `jt_customer` | `staging/uc_6/raw/` | `CM_JT_Customer_Details_*.dat.gz` | gzip CSV, pipe, headerless, 34 fields |
| `excalibur_address` | `staging/uc_6/raw/` | `CM_EXCALIBUR_ADDRESS_*.dat.gz` | gzip CSV, **comma**, headerless, 43 fields |

**The `CSS_account_[0-9]*` pattern is deliberate and must not be simplified.** `CSS_account_*` also
matches `CSS_account_address_*`, whose 19-field schema would corrupt the 56-field account table.

**EA request pre-processing chain:** PGP-symmetric decrypt (passphrase from
`br_digital_poc.config.pgpkey`) → gzip decompress → land in `_extracted/ea_request/` → Auto Loader
reads. `delete_source_after_extract` is `delete_now`, so **the raw file is consumed on every run and
must be re-uploaded before each run.**

The five positional feeds each carry an external schema config from
`BT_Usecase/UC6/onboarding/schema_configs/`, uploaded to `uc_6/_schema_configs/`.

### 3.2 Job and pipeline

| Job | Pipeline triggered | Tasks in order |
|---|---|---|
| `007_lfj_uc6_ea_flood_warning` | `008_ldp_uc6_ea_flood_warning` | `setup_control_tables` → `onboard_uc6` → `run_pipeline_update` → `apply_governance_uc6` → `observability_export` |

Dataflow group: `dfg_uc6_ea_flood_warning`. Pipeline parameters include
`match_strength_threshold=50`, `min_addresses_per_area=1`, `min_age_years=17`,
`excluded_business_unit_code=BS`.

### 3.3 Tables by layer, and their DAG role

#### Bronze — six streaming tables, all APPEND

| Table | Written by | Upstream | DAG role |
|---|---|---|---|
| `bronze.ea_request` | `df_uc6_ea_request_ingest` | decrypted `_extracted/ea_request/*.csv` | **Upstream input** to two Silver views. 2 fail + 3 warn DQ rules. |
| `bronze.css_account` | `df_uc6_css_account_ingest` | `raw/CSS_account_[0-9]*` | **Upstream input** to the PAF Silver view. |
| `bronze.css_account_address` | `df_uc6_css_account_address_ingest` | `raw/CSS_account_address_*` | as above |
| `bronze.css_subscription` | `df_uc6_css_subscription_ingest` | `raw/CSS_subscription_*` | as above |
| `bronze.jt_customer` | `df_uc6_jt_customer_ingest` | `raw/CM_JT_Customer_Details_*` | as above |
| `bronze.excalibur_address` | `df_uc6_excalibur_address_ingest` | `raw/CM_EXCALIBUR_ADDRESS_*` | as above |

**UC6 has no quarantine tables** — every ingestion flow sets `quarantine_table: null`.

#### Silver — four materialized views, TRUNCATE_AND_LOAD

| Table | Written by | Upstream | DAG role |
|---|---|---|---|
| `silver.ea_request_base` | `ts_uc6_ea_base` | `bronze.ea_request` | **Intermediate.** Dedupes to one row per `osapr` by latest ingestion timestamp. Feeds Gold OSAPR. |
| `silver.ea_request_address` | `ts_uc6_ea_address` | `bronze.ea_request` | **Intermediate.** Normalises postcode, address and town; flags PO boxes. Feeds the match view. |
| `silver.ee_customer_address_paf` | `ts_uc6_ee_address_paf` | **five Bronze tables** (`css_subscription`, `css_account`, `css_account_address`, `jt_customer`, `excalibur_address`) | **Fan-in.** Unifies customer PAF address + MSISDN across all five feeds into one shape with a `source_system` column. |
| `silver.flood_area_matched_address` | `ts_uc6_matched_address` | `silver.ea_request_address` + `silver.ee_customer_address_paf` | **The join.** Inner join on normalised postcode; computes `match_strength` as a token-intersection percentage. Feeds both Gold views. |

#### Gold / Semantic — two materialized views

| Table | Written by | Upstream | DAG role |
|---|---|---|---|
| `gold.flood_warning_osapr` | `ts_uc6_osapr_output` | `silver.ea_request_base` + `silver.flood_area_matched_address` | **Target consumer.** Applies the match-strength and address-count thresholds; emits `count` and `status` (`Single Addr`, `Bad OSAPR`, …). Feeds two sinks and the telephone view. |
| `gold.flood_warning_telephone` | `ts_uc6_telephone_output` | `silver.flood_area_matched_address` + `gold.flood_warning_osapr` | **Target consumer.** Distinct `targetAreaID, telephone`. Feeds two sinks. |

#### Gold — four egress sinks

| Sink | Upstream | Output file | Encrypted |
|---|---|---|---|
| `gold.leidos_telephone_export` | `gold.flood_warning_telephone` | `EE_<date>-LEIDOS_TELEPHONE_<seq>` | No — gzip only |
| `gold.leidos_osapr_export` | `gold.flood_warning_osapr` | `EE_<date>-LEIDOS_OSAPR_<seq>` | No — gzip only |
| `gold.telephone_export` | `gold.flood_warning_telephone` | `EE_<date>-TELEPHONE_<seq>` | **Yes** — `config.pgpkey` |
| `gold.osapr_export` | `gold.flood_warning_osapr` | `EE_<date>-OSAPR_<seq>` | **Yes** — `config.pgpkey` |

All four stage through `uc_6/output/_staging/<sink>/` and land ZIPs in `uc_6/output/`.

**Sink tags are declared but never applied.** The four sinks carry a complete `table_tags` block, but
the governance engine iterates ingestion and transformation rows only and emits `ALTER TABLE`. A sink
writes files to a volume, so there is no table to alter. The tags validate and then do nothing.

#### Presence gate — one `COUNT(*)` materialized view

`silver.flood_warning_source_presence` (`ts_uc6_source_presence_gate`): one row per bronze source
with its row count and a `row_count > 0` / `fail` expectation. The three flows that read bronze
directly cross-join it, so an empty delivery fails the update before any downstream table is
recomputed. UC6 declares **no** `reconciliation_flows` since 2026-09-08 (six self-comparison
`rf_uc6_<feed>_presence` flows before that); the `onboard_uc6` task passes
`prune_missing_flows: "true"` so their control rows are soft-disabled on re-onboarding.

### 3.4 Other UC6 assets

| Asset | Type | Provenance | Notes |
|---|---|---|---|
| `br_digital_poc.staging.uc_6` | Volume | **MANUAL** | Created by the setup notebook. |
| `uc_6/raw/`, `archive/`, `output/`, `_schemas/`, `_extracted/` | Paths | Notebook | |
| `uc_6/_schema_configs/` | Path | **MANUAL** | Uploaded by hand from the repository. |
| `br_digital_poc.config.pgpkey` | Secret | **MANUAL** | Shared ingress and egress passphrase. |

---

## Part 4 — UC7: Call detail records

### 4.1 Sources

All four are BER-encoded concatenated-TLV `.raw` files, `source_type: asn1`, `asn1_codec: ber`.

| Feed | Landing path | ASN.1 module |
|---|---|---|
| EMSC | `landing/uc_7/raw/EMSC/` | `asn_schema/EMSC.asn1` (module MSC12A, root PDU `CallDataRecord`) |
| PSGW | `landing/uc_7/raw/PSGW/` | `asn_schema/PSGW.asn1` |
| SGSN | `landing/uc_7/raw/SGSN/` | `asn_schema/SGSN.asn1` |
| TAP | `landing/uc_7/raw/TAP/` | `asn_schema/TAP.310.asn1` |

### 4.2 Job and pipeline

| Job | Pipeline triggered | Tasks in order |
|---|---|---|
| `001_lfj_uc7_cdr_asn` | `001_ldp_uc7_cdr_asn` | `setup_control_tables` → `onboard_uc7` → `run_pipeline_update` → `observability_export` |

Dataflow group: `dfg_uc7_cdr_asn`.

**UC7 is the only use-case job with no `apply_governance` task**, and all four of its ingestion flows
declare `"governance_tags": {}`. The most sensitive data in the estate therefore carries no tags at
all. Populating the tags without also adding the task achieves nothing — the tag DDL is applied by
the task, not by the pipeline.

**The pipeline hardcodes its catalog** rather than using `${var.catalog}`. Until that is
parameterised, UC7 cannot be pointed at a different catalog by changing a variable alone.

### 4.3 Tables by layer, and their DAG role

UC7 has **no transformation flows and no reconciliation flows** — zero Silver, zero Gold. It is a
Bronze-only decode-and-quarantine use case.

| Table | Layer | Written by | Upstream | DAG role |
|---|---|---|---|---|
| `bronze.emsc_cdr_raw` | Bronze | `df_uc7_emsc_cdr_ingest` | EMSC BER files | **Terminal target consumer.** Nothing downstream reads it in-pipeline. |
| `bronze.psgw_cdr_raw` | Bronze | `df_uc7_psgw_cdr_ingest` | PSGW BER files | as above |
| `bronze.sgsn_cdr_raw` | Bronze | `df_uc7_sgsn_cdr_ingest` | SGSN BER files | as above |
| `bronze.tap310_raw` | Bronze | `df_uc7_tap310_ingest` | TAP BER files | as above |
| `bronze.emsc_cdr_raw_quarantine` | Quarantine | same flow | shared staged view | **Sibling reject path.** Rows failing the three decode rules. |
| `bronze.psgw_cdr_raw_quarantine` | Quarantine | same flow | shared staged view | as above |
| `bronze.sgsn_cdr_raw_quarantine` | Quarantine | same flow | shared staged view | as above |
| `bronze.tap310_raw_quarantine` | Quarantine | same flow | shared staged view | as above |

Each feed carries three quarantine rules — decode-ok, choice-arm-selected and arm-populated — keyed
on `_choice`. The presence of a `quarantine` action forces a materialized staged view intermediate,
which is what lets the pass and reject paths share **one** read of the source.

### 4.4 The deliberate SMSC and MMSC exclusion

Two further network elements exist in the source estate and are intentionally **not** onboarded.
Their payloads are CSV text, not ASN.1 — SMSC is a single-line 48-field quoted CSV, MMSC a 70-plus
column CSV — there is no `SMSC.asn1` module at all, and both fail to decode against every available
module with a `DecodeTagError`. Onboarding them as `source_type: asn1` would quarantine 100 per cent
of their rows **while reporting success.**

### 4.5 Other UC7 assets

| Asset | Type | Provenance | Notes |
|---|---|---|---|
| `br_digital_poc.landing.uc_7` | Volume | **MANUAL** | Created by the setup notebook. |
| `uc_7/asn_schema/` | Path | Notebook | The four `.asn1` module files. |
| `uc_7/_schemas/` | Path | Notebook | Auto Loader schema checkpoints. |
| `uc_7/output_sample/`, `archive/` | Paths | **MANUAL** | Not created by the notebook. |

**`SGSN.asn1` is in no part of this repository** and must be supplied by the customer straight into
the upload folder, even though the setup notebook validates for it and the spec references it.
Conversely `GGSN.asn1` and `TAP.311.asn1` *are* in the repository but no flow references them — they
are generator inputs, not pipeline schemas.

---

## Part 5 — Dashboards and observability

### 5.1 AI/BI dashboard

**One dashboard exists in the estate.** There is no UC3-, UC6- or UC7-specific dashboard.

| Attribute | Value |
|---|---|
| Display name | `FlowX Control Metadata Dashboard` |
| Dashboard file | `databricks-bi/flowx_control_metadata_dashboard.lvdash.json` |
| Resource | `resources/flowx_bi/flowx_control_dashboard.yml`, key `flowx_control_dashboard` |
| Provenance | Bundle-managed |
| Warehouse | `${var.dashboard_warehouse_id}` — no portable default; must be set per target |
| Dataset binding | `dataset_catalog: ${var.catalog}`, `dataset_schema: config` |
| Pages | Overview · Global Filters · Ingestion Flows · Transformation Flows · Reconciliation · Observability & Audit · Raw JSON |

**Queries deliberately use bare table names.** The catalog and schema are injected at deploy time by
`dataset_catalog` / `dataset_schema`, which is what lets one dashboard file serve every target.
Hardcoding a three-part name breaks that and is a review-blocking defect.

The dashboard reads **six of the nine** control tables — `dataflow_group_spec`,
`ingestion_flow_spec`, `transformation_flow_spec`, `reconciliation_flow_spec`,
`onboarding_audit_log` and `reconciliation_run_log`. It reads **no** use-case data table, so it
carries no sensitivity of its own beyond control metadata.

Its eight datasets: `filter_dataflow_groups`, `main_audit_flow_unfiltered`, `kpi_metrics_filtered`,
`ingestion_flows_detail`, `reconciliation_detail`, `transformation_flows_detail`,
`audit_log_detail`, `raw_spec_json`.

### 5.2 Observability export destinations

Every use-case job ends with an `observability_export` task writing to the observability volume.

| Use case | Destination path | Format |
|---|---|---|
| UC3 streaming CDC | `observability/app_logs/streaming_cdc` | JSON |
| UC3 batch recon | `observability/app_logs/batch_recon` | JSONL + gzip |
| UC6 | `observability/app_logs/uc6` | JSONL + gzip |
| UC7 | `observability/app_logs/` | JSONL + gzip |

---

## Part 6 — Governance: tagging, sensitivity and access

### 6.1 How tags are applied

The framework's own route is preferred over hand-written DDL:

onboarding spec `governance_tags` → `governance_tags_json` in the control tables → the
`apply_governance` job task → `ALTER TABLE … SET TAGS`.

Two rules are enforced at validation: exactly two keys are allowed (`column_tags`, `table_tags`), and
**values must be strings** — `"pii": true` is rejected, `"pii": "true"` is required.

Three structural limits follow from this design:

1. **Only ingestion and transformation flows are tagged.** Reconciliation flows and sinks are not.
2. **Tagging is opt-in and silent when absent.** A flow with no tags produces no warning — UC7's four
   empty blocks pass validation cleanly.
3. **The DDL needs its own job task.** Without `apply_governance` in the job, spec tags are inert.

Container-level tags (catalog, schema, volume) have no spec route and are applied from
[`setup_scripts/01_container_tags.sql`](setup_scripts/01_container_tags.sql). The full key
vocabulary and the tagging constraints are defined in
[document 03](03_platform_best_practice_and_naming_standards.md), clause 4.

### 6.2 Data sensitivity

| Use case | Classification | Basis |
|---|---|---|
| **UC7** | **RESTRICTED** | Call detail records carrying MSISDN, IMSI, IMEI and cell-site location — communications metadata, i.e. location history and calling patterns. The most sensitive data in the estate. |
| **UC6** | **CONFIDENTIAL** | GPG-encrypted customer data which is, by its purpose, a register of vulnerable people at flood risk. Sensitive by inference as much as by content. |
| **UC3** | **CONFIDENTIAL**, with **RESTRICTED** columns | Telecoms reference data containing credentials and a credit-card column. |

**A UC3 credential-column caveat.** Four columns — `acc_password`, `blacklist_password`,
`imei_black_list_pass` and `gur_cr_card_no` — are tagged `NULL_AT_SOURCE`, and the streaming spec
casts them to `NULL`. On the streaming path the sensitive values never land. **This was verified on
the streaming flows only.** If the batch path does not do the same, it lands credentials and a card
number that the streaming path deliberately discards — and the reconciliation between the two would
compare a nulled column against a populated one. Verify the batch path before rollout.

### 6.3 Access control

Grants are consolidated in [`setup_scripts/03_grants.sql`](setup_scripts/03_grants.sql). Two
prohibitions are worth repeating here because they are easy mistakes with real consequences:

1. **Never `GRANT SELECT ON SCHEMA br_digital_poc.bronze` to any consumer group.** That schema mixes
   all three use cases, so a grant intended to give a team its own Bronze tables also hands it UC7's
   call detail records. Grant at table granularity in this schema, always.
2. **Never grant `READ VOLUME` on `uc_6` broadly.** The `output/` subtree holds egress files destined
   for external parties, and volume grants are **not path-scoped** — there is no way to grant `raw/`
   without also granting `output/`.

Column masks and row filters do not exist. The proposed shapes, explicitly marked not-applied, are in
[`setup_scripts/02_column_masks_and_row_filters.sql`](setup_scripts/02_column_masks_and_row_filters.sql).

---

## Part 7 — Repository source data and the UC6 test fixture

### 7.1 Provenance classification

A Unity Catalog table can be rebuilt from its source; a customer-supplied source file that is
regenerated is gone. Every data asset is therefore marked **[Customer-Provided]** or **[Simulated]** —
a statement about whether a repository script can reproduce the file, not a judgement about realism.
**A [Customer-Provided] file has no generator and must never be regenerated, overwritten or
"refreshed".**

| Use case | Asset | Class | Basis |
|---|---|---|---|
| **UC3** | `UC3/data/{CUSTOMER,SUBSCRIBER,PHYSICAL_DEVICE}_DDL.csv` | **[Customer-Provided]** | The three Excalibur governance sheets. `scripts/generate_uc3_test_data.py` **reads** them to learn column names, types and governance flags; it never writes them. A generator's input is not its output. |
| **UC3** | `build/uc3_test_data/**` | **[Simulated]** | Written by `scripts/generate_uc3_test_data.py`. `build/` is gitignored scratch, so the CSVs are generated on demand. Fully reproducible. |
| **UC6** | `UC6/data/sample_bundle/**` | **[Customer-Provided]** | The supplied `uc_6_poc_bundle.zip`, sanitised **by the customer at source**. No repository script generates it. |
| **UC6** | `UC6/data/test_fixture/**` | **[Simulated]** — except one file, see 7.3 | Written by `scripts/generate_uc6_test_data.py`. |
| **UC7** | `UC7/data/asn_schema/*.asn1` | **[Customer-Provided]** | Real ASN.1 protocol module definitions; generator input. |
| **UC7** | `UC7/data/tap311_sample.ber` | **[Customer-Provided]** | Supplied sample payload. |
| **UC7** | `UC7/data/synthetic/*.ber` | **[Simulated]** | Written by `scripts/generate_synthetic_ber.py`, ten records per protocol, deterministic. |

### 7.2 Why `UC6/data/test_fixture/**` exists

**The fixture is not a stand-in for missing data.** The customer bundle was supplied, in full and on
time. The fixture exists because the supplied bundle **cannot exercise the business logic**, for two
independent and verified reasons:

1. **No postcode overlap.** The EA request postcodes (`AB12 3CD`, `EF45 6GH`, `IJ78 9KL`, `MN10 2OP`)
   share nothing with the CSS (`RH2 9QQ`), Excalibur (`HA61BL`) or JT (`TN393QN`) postcodes.
2. **No CSS join-key overlap.** Account ids run ~2873–3051, subscription ids ~3296–3715, address ids
   ~46649141. There is zero overlap on every candidate join column — the three CSS files were
   evidently generated independently of each other.

A run over the supplied bundle therefore produces **zero matches** and can only ever exercise the
no-match path. The acceptance test in the customer brief cannot pass on the bundle as shipped.

The generated fixture is engineered so every branch of the decision table is hit by at least one row
— `AREA_FOUND` → Found, `AREA_NOTFOUND` → Not Found, `AREA_BADOSAPR` → Bad OSAPR, `AREA_SINGLE` →
Single Addr — plus two deliberate negative cases: a PAYG customer under 17 to prove the age filter
excludes, and a postcode that matches with unrelated address text to prove the match-strength
threshold rejects it. As a privacy safeguard the telephone list contains only `AREA_FOUND` MSISDNs.

**Both trees are retained deliberately.** The supplied bundle proves the real-world no-match path,
and its zero-match behaviour is *pinned by a test* so nobody mistakes it for a defect. The generated
fixture proves every branch. Deleting either loses a distinct test.

### 7.3 Fixture file inventory

All six generated files are written by `scripts/generate_uc6_test_data.py` into
`BT_Usecase/UC6/data/test_fixture/`.

| File | Naming pattern | Format | Delimiter | Fields | Rows |
|---|---|---|---|---|---|
| `CSS_account_20260820_00000001.dat.gz` | `<FEED>_<YYYYMMDD>_<8-digit seq>.dat.gz` | gzip, headerless | `\|` | 56 | 5 |
| `CSS_subscription_20260820_00000001.dat.gz` | as above | gzip, headerless | `\|` | 47 | 5 |
| `CSS_account_address_20260820_00000001.dat.gz` | as above | gzip, headerless | `\|` | 19 | 5 |
| `CM_JT_Customer_Details_20260820_00000001.dat.gz` | as above | gzip, headerless | `\|` | 34 | 1 |
| `CM_EXCALIBUR_ADDRESS_20260820_00000001.dat.gz` | as above | gzip, **comma** | `,` | 43 | 1 |
| `EE_2026-08-20-REQUEST_1OF1.csv.gz.gpg` | `EE_<YYYY-MM-DD>-REQUEST_<n>OF<n>.csv.gz.gpg` | gzip **then GPG symmetric (AES256)** | `\|` | 14, **with header** | 7 |

Only the EE file is encrypted; the other five are plain gzip. Encryption is **symmetric — a
passphrase, not a keypair** — and the POC passphrase is the same one that decrypts the customer's
own EE file.

### 7.4 The EE file: status and the corrected location

**The official Environment Agency request file was supplied by the customer. It was not delayed,
missing or replaced by a placeholder.** No document in the repository claims otherwise.

There are two *distinct* EE files, and conflating them is the trap:

| File | Class | Content |
|---|---|---|
| `test_fixture/EE_2026-08-20-REQUEST_1OF1.csv.gz.gpg` (787 B) | **[Simulated]** | Generated. Carries the four `AREA_*` target-area IDs that drive the decision-table branches. |
| `test_fixture/EE_2026-08-20-REQUEST_1OF1.customer_supplied.csv.gz.gpg` (625 B) | **[Customer-Provided]** | The supplied file: hex-string target-area IDs, richer PAF fields. Plaintext-identical to the copy in `sample_bundle/`. |

**Path correction applied.** The customer-supplied copy previously sat at
`BT_Usecase/UC7/data/EE_2026-08-20-REQUEST_1OF1.csv.gz.gpg` — a stray from the v0.0.4 consolidation.
It is an Environment Agency artefact and belongs to UC6: no UC7 spec, script or job references it,
and UC7 ingests only BER-encoded ASN.1. It has been moved under
`BT_Usecase/UC6/data/test_fixture/` and given the `.customer_supplied` qualifier so it cannot be
confused with the generated file of the same base name. `UC7/data/` now holds only `asn_schema/`,
`synthetic/` and `tap311_sample.ber`.

> Its ciphertext differs from the `sample_bundle/` copy while the plaintext is identical — PGP
> symmetric encryption uses a fresh random session key per invocation, so the same bytes encrypt to
> different ciphertext every time. This is expected, not evidence of a different file.

The authoritative classification is
[`docs/DATA_PROVENANCE_CLASSIFICATION.md`](../../../docs/DATA_PROVENANCE_CLASSIFICATION.md); every
script named here is indexed in [`docs/SCRIPTS_GUIDE.md`](../../../docs/SCRIPTS_GUIDE.md).

---

## Part 8 — Summary counts

| Category | UC3 | UC6 | UC7 | Shared | Total |
|---|---|---|---|---|---|
| Sources | 6 | 6 | 4 | — | 16 |
| Volumes | 1 | 1 | 1 | 3 | 6 |
| Jobs | 3 | 1 | 1 | 2 | 7 |
| Pipelines | 2 | 1 | 1 | — | 4 |
| Dataflow groups | 2 | 1 | 1 | — | 4 |
| Bronze tables | 3 | 6 | 4 | — | 13 |
| Silver tables | — | 4 | — | — | 4 |
| Gold tables / MVs | — | 2 | — | — | 2 |
| Sinks | — | 4 | — | — | 4 |
| Staging tables | 6 | — | — | — | 6 |
| Quarantine tables | — | — | 4 | — | 4 |
| Control tables | — | — | — | 9 | 9 |
| Dashboards | — | — | — | 1 | 1 |
| Secrets | 0 | 1 | 0 | — | 1 |

Schemas are deliberately omitted from the per-use-case count because they overlap: `bronze` is shared
by all three.

---

## The delivery set

| # | Document | Covers |
|---|---|---|
| 01 | Use case asset inventory and governance | *This document* |
| 02 | [Environment deployment and setup](02_environment_deployment_and_setup.md) | The setup and staging notebook, its design, and how to run it |
| 03 | [Databricks naming and best practice standards](03_platform_best_practice_and_naming_standards.md) | Naming templates, tag vocabulary, build practice |
| 04 | [Consolidated rollout runbook](04_consolidated_rollout_runbook.md) | The ordered end-to-end sequence |

Setup DDL referenced above lives in [`setup_scripts/`](setup_scripts/).

---

*End of document 01.*
