# 🧪 Metaflow — Master Testing Plan & Pipeline-Job Architecture

> **Role & Authority**: Databricks Solutions Architect & Lakeflow Framework Specialist  
> **Target Platform**: Databricks Lakeflow Declarative Pipelines (Delta Live Tables), Unity Catalog, Delta Lake, Databricks Asset Bundles (DAB)  
> **Document Location**: `metaflow_testing/TESTING_PLAN.md`  
> **Catalog Scope**: `{{catalog}}` (e.g., `metaflow`, `poc`, `dev`)  

---

## 0. Live Execution Status — `dev_metaflow` workspace (2026-08-29)

> **Read this before treating any row in §3 as verified.** The tables in §3 are the *planned*
> specification. This section records what has actually been executed against the
> `dev_metaflow` workspace (`dbc-2f6b7d4f-8c5b.cloud.databricks.com`, catalog `metaflow`,
> profile `<redacted-profile>`) and what the failures really were.
> Authoritative per-test detail lives in [`TESTING_STATUS.md`](TESTING_STATUS.md) §0.

### Targeted pipeline-failure investigation (latest pass)

Five specifically-named failing pipelines were diagnosed from their own Lakeflow event streams
(not from runner logs) and fixed. **Every one of the five turned out to be a real defect** —
none was the harness artefact the earlier taxonomy assumed — and four of the five were defects
this release introduced or left latent.

**Outcome: 5 of 5 now PASS on `dev_metaflow`.**

| Pipeline / test | Root cause established from live events | Fix | Verified |
|---|---|---|---|
| `metaflow_test_ing_004_asn1_decode` | `[CF_UNKNOWN_OPTION_KEYS_ERROR] cloudFiles.filenamepattern`. `cloudFiles.fileNamePattern` **is not a valid Auto Loader option for any format** — the `cloudFiles.` key whitelist is checked without consulting `cloudFiles.format`. The earlier "make it format-aware" fix only corrected `binaryFile`, leaving every other format on the invalid spelling. | `_apply_common_autoloader_options` now emits the generic, un-prefixed `pathGlobFilter` for **all** formats. | ✅ PASS |
| `metaflow_test_002_zerobus` | `DELTA_SOURCE_TABLE_IGNORE_CHANGES`. The seeder MERGEd into `zerobus_source_bus` with `whenMatchedUpdate()`, rewriting already-present rows on every re-seed. A Delta **streaming source must be append-only**, so the second seed permanently poisoned the stream. | Both zerobus seeders are now **insert-only**; one full refresh cleared the historical MERGE commit. | ✅ PASS (incl. end-to-end 002-003 job) |
| `metaflow_test_cdc_002_truncate` | `Graph is not topologically sorted. There is a cycle between dim_fx_rates_current and dim_fx_rates_current`. v1.3.0's E09 empty-source guard made the target read **itself**. | E09's in-graph guard **withdrawn** — see below. | ✅ PASS |
| `metaflow_test_cdc_006_snapshot_pk` | Three stacked failures, each revealed by fixing the previous: `TABLE_OR_VIEW_NOT_FOUND` → `REFERENCE_DLT_DATASET_OUTSIDE_QUERY_DEFINITION` → `View ... is a streaming view and must be referenced using readStream`. A lambda passed to `apply_changes_from_snapshot` **may not reference any pipeline dataset**. | Snapshot input is now a real `@dlt.table` dataset (filter + NO_PK guard inside it), passed to `apply_changes_from_snapshot` **by name**; `is_streaming` threaded down so the upstream is read with the matching API. | ✅ PASS |
| `metaflow_test_cdc_007_snapshot_nopk` | Same as CDC-006, then a `setup_control_tables` race: concurrent jobs hit `[ROUTINE_ALREADY_EXISTS]` on `preflight_check_onboarding_spec`. UC's `CREATE OR REPLACE FUNCTION` is idempotent in intent but **not atomic**. | Same snapshot fix, plus a narrow `is_already_exists_race()` tolerance shared by the provisioner and the setup notebook. | ✅ PASS |

### E09 (`empty_target_if_source_empty`) is WITHDRAWN

The guard preserved a `TRUNCATE_AND_LOAD` target by recomputing it from its own previous
contents — i.e. the dataset reads itself, which Lakeflow rejects outright. Its eager emptiness
test is independently unusable: `_clean_upstream` runs during graph **construction**, when an
upstream produced by the same update legitimately holds no data, so "source is empty" and
"source not yet materialized" cannot be told apart. **Every `TRUNCATE_AND_LOAD` pipeline failed
100% of the time.** The option remains valid in specs but has no runtime effect; enforcing it
needs a post-update check outside the pipeline graph. See `docs/03_transformation_and_cdc.md`.

### Known limitation: snapshot CDC over an append-only landing zone

`apply_changes_from_snapshot` treats its source dataset's *current contents* as the latest full
snapshot. With a streaming Auto Loader upstream those contents **accumulate**, so once a Day-2
extract lands the dataset holds Day-1 ∪ Day-2: keys dropped in Day-2 are never deleted and
modified keys appear twice. Single-snapshot flows are correct; the Day-1/Day-2 diffing described
in §3 for `TC-CDC-006`/`TC-CDC-007` is **not** supported by this wiring and cannot be, because no
named dataset can mean "only the most recently arrived snapshot". The supported Lakeflow pattern
is the lambda form reading a versioned **path** per snapshot (legal, because a path is not a
pipeline dataset). Adopting it needs a new spec contract for locating snapshot versions and
changes how DQ/quarantine applies to snapshot flows — deliberately not attempted here.

### Earlier full-suite pass: failure taxonomy

| Cause | Count | Framework defect? | Detail |
|---|---|---|---|
| `Pipeline update already in progress` | 8 | **No** — test-harness defect | Several `TC-*` cases deliberately **share a Lakeflow pipeline** (e.g. `TC-GOV-002` reuses `TC-GOV-001`'s pipeline). The wave runner ran them concurrently. **Cases that share a pipeline must be serialised.** |
| `ENVIRONMENT_PIP_INSTALL_ERROR` | 3+ | **No** — build/deploy infrastructure | A `bundle deploy` during a running update replaced the wheel being installed. Fixed structurally by publishing wheels to a UC Volume (`/Volumes/<catalog>/framework/wheels/`) pinned via the `framework_wheel_path` bundle variable. |
| `[ROUTINE_ALREADY_EXISTS]` in `setup_control_tables` | 1 | **Yes** (now fixed) | Concurrent jobs racing UC function creation — see the table above. |
| **Expected-failure test misclassified** | 1 | **No** — harness defect | See `TC-DQ-003` below. |

### `TC-DQ-003` is a PASS, not a failure

`TC-DQ-003`'s own assertion in §3 Module 4 reads: *"Pipeline update status returns `FAILED`.
Task 1 in workflow job fails with `ExpectationViolationException`."* The live run produced
exactly that. The wave runner classifies purely on job-level terminal state, so it recorded a
FAIL. **For any negative/expected-failure test the job status must be inverted.** Affected rows:
`TC-DQ-003` and `TC-PRM-006`.

### Runner requirements this run established

1. **Serialise pipeline-sharing cases.** Concurrency > 1 across cases sharing a pipeline
   produces `Pipeline update already in progress`.
2. **Never `bundle deploy` while a wave is running** — jobs launched before the deploy still run
   the previous wheel, so results are attributed to the wrong build.
3. **Invert the assertion for expected-failure tests** (`TC-DQ-003`, `TC-PRM-006`).
4. **Keep concurrency ≤3 overall** on a free-tier workspace to avoid `RESOURCE_EXHAUSTED` /
   `QUOTA_EXCEEDED` (~50 schemas per catalog, ~50 volumes per metastore).
5. **A `bundle deploy` can silently skip changed notebooks.** DABs' sync snapshot under
   `.databricks/bundle/<target>/sync-snapshots/` goes stale and reports `Files: 0 uploaded` while
   the workspace keeps running the old notebook. Delete it and redeploy, then verify with
   `databricks workspace export <path>/notebooks/<dir>/<name> --format SOURCE` (**no `.py`
   extension** on the deployed path).

---

## 1. Executive Summary & Testing Framework Architecture

The **NextGen Metadata Framework (Metaflow)** is a configuration-driven ETL/ELT platform on Databricks Lakeflow Declarative Pipelines. 

### Core Testing Mandate
To ensure enterprise-grade reliability, configuration flexibility, and zero feature regression:
1. **Isolated Pipeline Per Test Case**: Every test scenario executes through its own dedicated, isolated Lakeflow Declarative Pipeline.
2. **Standard 2-Task Workflow Job Wrapper**: Each pipeline is wrapped inside a dedicated Databricks Workflow Job containing exactly two chained tasks:
   * **Task 1 (`run_pipeline_task`)**: Executes the specific Lakeflow Declarative Pipeline update.
   * **Task 2 (`run_observability_task`)**: Executes the DLT Observability Engine (`08_dlt_observability_engine.py`), consuming the dynamic pipeline ID `${resources.pipelines.<pipeline_name>.id}` to extract DLT event logs, validate execution telemetry, and emit OpenTelemetry (OTEL) payloads.
3. **End-to-End Parameterization & Dynamic Templating**: Comprehensive evaluation of dynamic parameters (`${param}`) and template variables (`{{catalog}}`, `{{env}}`) across all system components: file paths, ZIP landing/staging volumes, transformation SQL filters, reconciliation filter conditions, self-healing SQL, DQ boundary expressions, sink egress file naming, and observability telemetry tags.
4. **Realistic Production Scenarios**: Tests include edge cases such as selective glob pattern matching (e.g., matching vs non-matching files in the same volume directory), schema evolution rescue routing, malformed data quarantining, encryption key rotation, and cross-dataset drift self-healing.
5. **Explicit Databricks Object Naming**: Full declaration of target Catalogs, Schemas, Tables, Companion Tables (`_quarantine`, `_current`), Volume paths, Secret scopes/keys, Pipeline resources, and Job resources.

---

## 2. Standard 2-Task DAB Workflow Execution Architecture

Every test scenario defined in this plan implements the following Databricks Asset Bundle (DAB) resource architecture:

```mermaid
flowchart LR
    subgraph DAB_WORKFLOW_JOB["Databricks Workflow Job: metaflow_test_<module>_<scenario>_job"]
        direction LR
        subgraph TASK_1["Task 1: run_pipeline_task"]
            T1[Lakeflow Pipeline Task<br><b>pipeline_id</b>: metaflow_test_&lt;scenario&gt;_pipeline]
        end
        
        subgraph TASK_2["Task 2: run_observability_task"]
            T2[Observability Notebook Task<br><b>notebook_path</b>: 08_dlt_observability_engine.py<br><b>parameter</b>: pipeline_id = ${resources.pipelines.metaflow_test_&lt;scenario&gt;_pipeline.id}]
        end

        TASK_1 -->|On Success| TASK_2
    end

    subgraph LAKEFLOW_RUNTIME["Lakeflow Declarative Execution (Task 1)"]
        direction TB
        E1[Source Volume / Delta Bus] --> E2[Bronze / Silver / Gold DAG]
        E2 --> E3[Materialized Tables & Egress Volumes]
        E2 -.->|System Telemetry| DLT_LOGS[(DLT Event Log)]
    end

    subgraph OBSERVABILITY_RUNTIME["Observability Engine Execution (Task 2)"]
        direction TB
        DLT_LOGS --> O1[Extract Events & Lineage]
        O1 --> O2[Build OTEL JSON Schema]
        O2 --> O3[Dispatch to Volume JSONL.gz & OTLP Endpoint]
    end

    TASK_1 -.-> LAKEFLOW_RUNTIME
    TASK_2 -.-> OBSERVABILITY_RUNTIME
```

### Standard DAB Resource Definition Template (YAML Reference)

```yaml
resources:
  pipelines:
    metaflow_test_<module>_<scenario>_pipeline:
      name: "Metaflow Test - <Module> - <Scenario> Pipeline"
      target: ${var.catalog}
      configuration:
        dataflow_group_id: "dfg_test_<module>_<scenario>"
        catalog: ${var.catalog}
        env: ${var.env}
      libraries:
        - notebook:
            path: ../notebooks/03_engine/03_lakeflow_declarative_pipeline.py

  jobs:
    metaflow_test_<module>_<scenario>_job:
      name: "Metaflow Test - <Module> - <Scenario> - 2-Task Workflow"
      tasks:
        - task_key: run_pipeline_task
          pipeline_task:
            pipeline_id: ${resources.pipelines.metaflow_test_<module>_<scenario>_pipeline.id}
          environment_key: framework_env

        - task_key: run_observability_task
          depends_on:
            - task_key: run_pipeline_task
          notebook_task:
            notebook_path: ../notebooks/08_observability/08_dlt_observability_engine.py
            base_parameters:
              pipeline_id: ${resources.pipelines.metaflow_test_<module>_<scenario>_pipeline.id}
              catalog: ${var.catalog}
              dataflow_group_id: "dfg_test_<module>_<scenario>"
          environment_key: framework_env

      environments:
        - framework_env:
          spec:
            environment_version: "4"
            dependencies:
              - ../dist/*.whl
```

---

## 3. Modular Testing Plan & Specification Tables

---

### Module 1: Ingestion Engine & Source Adapters

Tests Auto Loader file discovery, regex/glob filtering, ZIP decompression/retention, Zerobus streaming Delta reads, binary ASN.1 telecom CDR decoding, landing PGP decryption, JSON flattening, column standardization, and technical metadata enrichment.

| Test Case No | Testing Functionality & Objective | Realistic Production Scenario | Databricks Object Names | Proposed Pipeline & Job Architecture | Expected Output & Verification Logic |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **TC-ING-001** | **Selective Glob Pattern ZIP Extraction**<br>Verify Auto Loader only extracts and ingests archives matching pattern, ignoring non-matching files. | Landing volume contains 2 files:<br>1. `customer_data_20260828.zip` (matches pattern `customer_*.zip`, has 100 rows).<br>2. `vendor_feed_20260828.zip` (does not match pattern, has 50 rows).<br>Framework must extract *only* `customer_*.zip` into staging and ingest 100 rows. `vendor_feed` must remain untouched in incoming volume. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `bronze_crm`<br>• **Target Table**: `customer_raw`<br>• **Landing Volume**: `/Volumes/{{catalog}}/crm/landing_zip/incoming/`<br>• **Extract Volume**: `/Volumes/{{catalog}}/crm/landing_zip/extracted/customer/`<br>• **Schema Vol**: `/Volumes/{{catalog}}/crm/_schemas/customer/` | • **Pipeline**: `metaflow_test_ing_001_zip_filter_pipeline`<br>• **Job**: `metaflow_test_ing_001_zip_filter_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` (`pipeline_id`, `catalog`, `dataflow_group_id`) | **SQL Assertion**:<br>`SELECT count(*) FROM {{catalog}}.bronze_crm.customer_raw;` → exactly 100 rows.<br>`vendor_feed` records appear nowhere in target table.<br>`vendor_feed_20260828.zip` remains intact in incoming Volume. |
| **TC-ING-002** | **ZIP Source Purge Retention Policy**<br>Verify `delete_source_after_extract: true` cleans incoming volume while keeping extracted files. | Incoming volume receives `orders_batch_01.zip`. Pipeline extracts CSV to extracted volume and deletes original `.zip` archive from incoming Volume upon successful unpack. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `bronze_sales`<br>• **Target Table**: `orders_raw`<br>• **Landing Volume**: `/Volumes/{{catalog}}/sales/landing_zip/incoming/`<br>• **Extract Volume**: `/Volumes/{{catalog}}/sales/landing_zip/extracted/orders/` | • **Pipeline**: `metaflow_test_ing_002_zip_retention_pipeline`<br>• **Job**: `metaflow_test_ing_002_zip_retention_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **Volume Verification**:<br>`orders_batch_01.zip` is removed from incoming volume.<br>Extracted CSV exists in extracted volume.<br>`orders_raw` contains expected rows. |
| **TC-ING-003** | **Zerobus Streaming Delta Feed Ingestion**<br>Verify direct streaming ingestion from a source Delta CDC bus table into a Bronze streaming table. | Source Delta table `zerobus_source_bus` has 1,000 baseline records. During pipeline run, 200 new records are appended to `zerobus_source_bus`. Pipeline must stream all 1,200 records into `zerobus_bronze`. | • **Catalog**: `{{catalog}}`<br>• **Source Schema**: `excalibur_usecase`<br>• **Source Table**: `zerobus_source_bus`<br>• **Target Schema**: `bronze_excalibur`<br>• **Target Table**: `zerobus_bronze`<br>• **Checkpoint**: `/Volumes/{{catalog}}/excalibur_usecase/_checkpoints/zerobus/` | • **Pipeline**: `metaflow_test_ing_003_zerobus_stream_pipeline`<br>• **Job**: `metaflow_test_ing_003_zerobus_stream_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT count(*) FROM {{catalog}}.bronze_excalibur.zerobus_bronze;` → 1,200.<br>Change Data Feed verifies exactly 200 streaming append events on update. |
| **TC-ING-004** | **ASN.1 Binary Telecom CDR Decoding**<br>Verify schema-based parsing of binary BER/ASN.1 telecom call logs into structured Delta columns. | Landing volume receives binary `.ber` CDR file with multi-level nested fields (Call Duration, IMSI, IMEI, Cell ID, Roaming Flags). ASN.1 decoder parses binary stream without byte corruption. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `bronze_telecom`<br>• **Target Table**: `cdr_stream_raw`<br>• **ASN.1 Schema File**: `/Volumes/{{catalog}}/telecom/schemas/gsm_cdr.asn`<br>• **Landing Volume**: `/Volumes/{{catalog}}/telecom/landing_cdr/incoming/` | • **Pipeline**: `metaflow_test_ing_004_asn1_decode_pipeline`<br>• **Job**: `metaflow_test_ing_004_asn1_decode_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT count(*) FROM {{catalog}}.bronze_telecom.cdr_stream_raw WHERE imsi IS NOT NULL AND call_duration_sec > 0;` matches binary record count.<br>Zero decoding exception rows in error log. |
| **TC-ING-005** | **Landing PGP Decryption via UC Secrets**<br>Verify automated decryption of armored `.zip.pgp` landing archives using private key and passphrase from UC Secrets. | Partner drops `financial_txns_202608.zip.pgp` encrypted with team public key into landing volume. Framework decrypts archive in-memory using UC secret key, unpacks CSV, and loads Bronze table. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `bronze_finance`<br>• **Target Table**: `txns_raw`<br>• **Secret Scope**: `metaflow_crypto_scope`<br>• **Secret Key**: `pgp_private_key_finance`<br>• **Landing Volume**: `/Volumes/{{catalog}}/finance/landing_pgp/` | • **Pipeline**: `metaflow_test_ing_005_pgp_decrypt_pipeline`<br>• **Job**: `metaflow_test_ing_005_pgp_decrypt_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`txns_raw` contains expected rows in plaintext.<br>No intermediate decrypted plaintext files persisted to permanent storage. |
| **TC-ING-006** | **Nested JSON Struct Flattening (`explode_columns`)**<br>Verify automatic flattening of nested JSON arrays and struct payloads into relational columns. | Ingest IoT device event JSON stream containing `{"device_id": "D1", "metrics": [{"sensor": "temp", "val": 22.5}, {"sensor": "pressure", "val": 101.3}]}`. Explode `metrics` array into discrete rows. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `bronze_iot`<br>• **Target Table**: `sensor_telemetry_flat`<br>• **Landing Volume**: `/Volumes/{{catalog}}/iot/landing_json/` | • **Pipeline**: `metaflow_test_ing_006_json_explode_pipeline`<br>• **Job**: `metaflow_test_ing_006_json_explode_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT count(*) FROM {{catalog}}.bronze_iot.sensor_telemetry_flat;` → 2 rows per device.<br>Columns `metrics_sensor` and `metrics_val` are properly mapped. |
| **TC-ING-007** | **Inline Data Standardization SQL**<br>Verify `data_standardization_sql` sanitizes string whitespace, casing, and formatted codes. | Ingest CSV with dirty values: `"  us-east  "`, `"Acme Corp LLC"`, `" invalid-email@test.com  "`. Expressions apply `TRIM(UPPER(region))`, `LOWER(TRIM(email))`. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `bronze_master`<br>• **Target Table**: `companies_standardized`<br>• **Landing Volume**: `/Volumes/{{catalog}}/master/landing_companies/` | • **Pipeline**: `metaflow_test_ing_007_standardize_pipeline`<br>• **Job**: `metaflow_test_ing_007_standardize_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT count(*) FROM {{catalog}}.bronze_master.companies_standardized WHERE region = 'US-EAST' AND email = 'invalid-email@test.com';` equals total row count. |
| **TC-ING-008** | **Technical Metadata Enrichment & Ordering**<br>Verify injection of all 8 `__framework_*` columns and assert business columns precede framework columns. | Ingest standard CSV feed. Target Delta table must append `_rescued_data`, `__framework_source_file_name`, `__framework_source_file_size`, `__framework_source_file_modification_time`, `__framework_ingestion_timestamp_utc`, `__framework_pipeline_run_id`, `__framework_record_id`. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `bronze_ea`<br>• **Target Table**: `departments_raw`<br>• **Landing Volume**: `/Volumes/{{catalog}}/ea/landing_csv/` | • **Pipeline**: `metaflow_test_ing_008_tech_metadata_pipeline`<br>• **Job**: `metaflow_test_ing_008_tech_metadata_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **Schema Assertion**:<br>Table schema inspection proves `dept_id`, `dept_name` are at column index 0, 1.<br>Technical metadata columns reside at trailing indices and have non-null values. |
| **TC-ING-009** | **Schema Evolution & Malformed Column Rescue**<br>Verify `schema_evolution_mode: rescue` isolates unexpected schema drift into `_rescued_data`. | Day-1 file has `[id, name]`. Day-2 file has unexpected column `[id, name, unannounced_flag]` plus a corrupt non-numeric string in a float column. Target must not fail. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `bronze_ops`<br>• **Target Table**: `feed_rescued_raw`<br>• **Landing Volume**: `/Volumes/{{catalog}}/ops/landing_drift/` | • **Pipeline**: `metaflow_test_ing_009_rescue_schema_pipeline`<br>• **Job**: `metaflow_test_ing_009_rescue_schema_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT count(*) FROM {{catalog}}.bronze_ops.feed_rescued_raw WHERE _rescued_data IS NOT NULL;` equals count of schema-drifted rows. |

---

### Module 2: Change Data Capture (CDC) Strategies

Tests the 7 CDC merge and history strategies implemented via `dlt.apply_changes()` and `apply_changes_from_snapshot()`.

| Test Case No | Testing Functionality & Objective | Realistic Production Scenario | Databricks Object Names | Proposed Pipeline & Job Architecture | Expected Output & Verification Logic |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **TC-CDC-001** | **Streaming Append-Only Fact Loading (`APPEND`)**<br>Verify facts/logs stream into target Delta table without key deduplication or history overhead. | Clickstream telemetry events land in micro-batches. Multiple events carry identical `user_id` and `timestamp`. Target table appends every event verbatim. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `bronze_web`<br>• **Target Table**: `page_clicks_stream`<br>• **Landing Volume**: `/Volumes/{{catalog}}/web/landing_clicks/` | • **Pipeline**: `metaflow_test_cdc_001_append_pipeline`<br>• **Job**: `metaflow_test_cdc_001_append_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT count(*) FROM {{catalog}}.bronze_web.page_clicks_stream;` equals sum of rows across all micro-batches. |
| **TC-CDC-002** | **Full Materialized View Snapshot (`TRUNCATE_AND_LOAD`)**<br>Verify small reference dimensions full-recompute on each batch without stale rows. | Daily FX currency rates table arrives as a full replacement extract (50 rows). Pipeline recomputes materialized view and discards superseded rates. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `silver_ref`<br>• **Target Table**: `dim_fx_rates_current`<br>• **Source Table**: `{{catalog}}.bronze_ref.fx_rates_raw` | • **Pipeline**: `metaflow_test_cdc_002_truncate_pipeline`<br>• **Job**: `metaflow_test_cdc_002_truncate_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT count(*) FROM {{catalog}}.silver_ref.dim_fx_rates_current;` equals exactly 50 rows. No historical accumulation. |
| **TC-CDC-003** | **SCD Type 1 Overwrite with Delete Mapping (`SCD1`)**<br>Verify SCD1 upserts latest customer attributes and physically drops records matching delete operation codes. | Day-1: Customer `C001` created with `tier: SILVER`.<br>Day-2: `C001` updated to `tier: PLATINUM`, `C002` sent with `cdc_op: DELETED`.<br>Target must hold 1 row for `C001` (PLATINUM) and 0 rows for `C002`. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `silver_crm`<br>• **Target Table**: `dim_customer_scd1`<br>• **Landing Volume**: `/Volumes/{{catalog}}/crm/landing_customer/` | • **Pipeline**: `metaflow_test_cdc_003_scd1_pipeline`<br>• **Job**: `metaflow_test_cdc_003_scd1_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT tier FROM {{catalog}}.silver_crm.dim_customer_scd1 WHERE customer_id = 'C001';` → `PLATINUM`.<br>`SELECT count(*) FROM {{catalog}}.silver_crm.dim_customer_scd1 WHERE customer_id = 'C002';` → 0. |
| **TC-CDC-004** | **SCD Type 2 Full History & Active View (`SCD2`)**<br>Verify SCD2 creates new version rows with `__START_AT`/`__END_AT` timestamps and updates companion `<target>_current` view. | Day-1: Employee `E101` in Dept `D1`.<br>Day-2: `E101` transferred to Dept `D2`.<br>Target table must contain 2 rows for `E101` (historical `D1` closed with `__END_AT`, active `D2` with `__END_AT IS NULL`). Companion view returns only active row. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `silver_hr`<br>• **Target Table**: `dim_employee_scd2`<br>• **Companion View**: `dim_employee_scd2_current`<br>• **Landing Volume**: `/Volumes/{{catalog}}/hr/landing_emp/` | • **Pipeline**: `metaflow_test_cdc_004_scd2_pipeline`<br>• **Job**: `metaflow_test_cdc_004_scd2_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT count(*) FROM {{catalog}}.silver_hr.dim_employee_scd2 WHERE employee_id = 'E101';` → 2.<br>`SELECT dept_id FROM {{catalog}}.silver_hr.dim_employee_scd2_current WHERE employee_id = 'E101';` → `D2`. |
| **TC-CDC-005** | **SCD Type 3 Current & Previous State (`SCD3`)**<br>Verify SCD3 tracks current and previous attribute values in dedicated columns (`current_status`, `previous_status`). | Customer status updates from `TRIAL` to `ACTIVE`, then from `ACTIVE` to `CHURNED`. Target stores `current_status = 'CHURNED'` and `previous_status = 'ACTIVE'`. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `silver_sub`<br>• **Target Table**: `dim_subscription_scd3`<br>• **Landing Volume**: `/Volumes/{{catalog}}/sub/landing_sub/` | • **Pipeline**: `metaflow_test_cdc_005_scd3_pipeline`<br>• **Job**: `metaflow_test_cdc_005_scd3_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT current_status, previous_status FROM {{catalog}}.silver_sub.dim_subscription_scd3 WHERE sub_id = 'S100';` → `CHURNED`, `ACTIVE`. |
| **TC-CDC-006** | **Full Snapshot Diffing with Natural PK (`FULL_SNAPSHOT_CDC`)**<br>Verify daily full dumps detect inserts, updates, and deletes via `apply_changes_from_snapshot`. | Day-1 snapshot contains keys `[1, 2, 3]`.<br>Day-2 snapshot contains keys `[2, 3_modified, 4]` (key 1 absent).<br>Target inserts key 4, updates key 3, and applies soft/hard delete to key 1. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `silver_inventory`<br>• **Target Table**: `inventory_snapshot_cdc`<br>• **Landing Volume**: `/Volumes/{{catalog}}/inventory/landing_dumps/` | • **Pipeline**: `metaflow_test_cdc_006_snapshot_pk_pipeline`<br>• **Job**: `metaflow_test_cdc_006_snapshot_pk_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT item_id FROM {{catalog}}.silver_inventory.inventory_snapshot_cdc;` contains keys 2, 3, 4 and excludes key 1. |
| **TC-CDC-007** | **Full Snapshot Diffing Without PK (`FULL_SNAPSHOT_CDC_NO_PK`)**<br>Verify daily full dumps lacking natural keys generate `__framework_surrogate_key` to track row-level state diffs. | Legacy mainframe file has no primary key. Entire row hash is computed as surrogate key. Day-2 dump removes 1 row and adds 1 row. Surrogate key engine tracks changes accurately. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `silver_legacy`<br>• **Target Table**: `mainframe_accounts_cdc`<br>• **Landing Volume**: `/Volumes/{{catalog}}/legacy/landing_mainframe/` | • **Pipeline**: `metaflow_test_cdc_007_snapshot_nopk_pipeline`<br>• **Job**: `metaflow_test_cdc_007_snapshot_nopk_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`__framework_surrogate_key` exists on all rows.<br>Row count matches net delta of Day-2 snapshot. |

---

### Module 3: Medallion Transformations & Complex SQL

Tests multi-input streaming/batch joins, `UNION ALL` consolidation, dynamic parameter substitution, and column decryption inside transformation queries.

| Test Case No | Testing Functionality & Objective | Realistic Production Scenario | Databricks Object Names | Proposed Pipeline & Job Architecture | Expected Output & Verification Logic |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **TC-TRF-001** | **Multi-Source Streaming-to-Batch 4-Way Inner Join**<br>Verify joining 1 streaming driving table against 3 static lookup tables correctly excludes unassigned entities. | Ingest fact table `assignments_raw` (streaming) and 3 dimension tables: `employees_raw`, `departments_raw`, `projects_raw` (batch). Employee `E005` has no assignment. Inner join must output exactly 5 matched rows and exclude `E005`. | • **Catalog**: `{{catalog}}`<br>• **Source Schema**: `bronze_ea`<br>• **Target Schema**: `silver_ea`<br>• **Target Table**: `ea_joined_assignments`<br>• **Source Tables**: `assignments_raw`, `employees_raw`, `departments_raw`, `projects_raw` | • **Pipeline**: `metaflow_test_trf_001_4way_join_pipeline`<br>• **Job**: `metaflow_test_trf_001_4way_join_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT count(*) FROM {{catalog}}.silver_ea.ea_joined_assignments;` → exactly 5 rows.<br>`SELECT count(*) FROM {{catalog}}.silver_ea.ea_joined_assignments WHERE employee_id = 'E005';` → 0. |
| **TC-TRF-002** | **Multi-Regional Ingestion `UNION ALL` Consolidation**<br>Verify consolidating heterogeneous regional tables into a single enterprise Silver dataset. | Region A lands `orders_na_raw` (500 rows). Region B lands `orders_eu_raw` (400 rows). Transformation executes `SELECT * FROM orders_na_raw UNION ALL SELECT * FROM orders_eu_raw`. | • **Catalog**: `{{catalog}}`<br>• **Source Schema**: `bronze_sales`<br>• **Target Schema**: `silver_sales`<br>• **Target Table**: `all_global_orders`<br>• **Source Tables**: `orders_na_raw`, `orders_eu_raw` | • **Pipeline**: `metaflow_test_trf_002_union_all_pipeline`<br>• **Job**: `metaflow_test_trf_002_union_all_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT count(*) FROM {{catalog}}.silver_sales.all_global_orders;` → exactly 900 rows.<br>Both regional codes represented. |
| **TC-TRF-003** | **In-DAG Column Decryption for Business Computations**<br>Verify encrypted Bronze columns are decrypted in-memory before executing transformation SQL calculations. | Bronze table `patient_visits_raw` stores AES-encrypted `billing_amount`. Transformation specifies `source_inputs[].decrypted_columns: ["billing_amount"]` and calculates running revenue totals. | • **Catalog**: `{{catalog}}`<br>• **Source Schema**: `bronze_health`<br>• **Target Schema**: `silver_health`<br>• **Target Table**: `patient_billing_summary` | • **Pipeline**: `metaflow_test_trf_003_decrypt_transform_pipeline`<br>• **Job**: `metaflow_test_trf_003_decrypt_transform_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT sum(billing_amount) FROM {{catalog}}.silver_health.patient_billing_summary;` returns exact numerical sum of decrypted plaintext values. |

---

### Module 4: Data Quality (DQ) Rules & Quarantine System

Tests the 4 DQ expectation actions (`warn`, `drop`, `fail`, `quarantine`), diagnostic metadata injection, and conditional quarantine companion table provisioning.

| Test Case No | Testing Functionality & Objective | Realistic Production Scenario | Databricks Object Names | Proposed Pipeline & Job Architecture | Expected Output & Verification Logic |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **TC-DQ-001** | **DQ Action: `warn` Non-Blocking Validation**<br>Verify invalid rows pass into target table while logging violation counts in event logs. | Feed 100 customer records where 15 have `age < 18` under rule `{"expression": "age >= 18", "action": "warn"}`. Target table must retain all 100 rows. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `bronze_crm`<br>• **Target Table**: `customer_warn_raw`<br>• **Landing Volume**: `/Volumes/{{catalog}}/crm/landing_dq_warn/` | • **Pipeline**: `metaflow_test_dq_001_warn_pipeline`<br>• **Job**: `metaflow_test_dq_001_warn_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT count(*) FROM {{catalog}}.bronze_crm.customer_warn_raw;` → 100.<br>Observability task records 15 expectation warning events. |
| **TC-DQ-002** | **DQ Action: `drop` Silent Row Filtering**<br>Verify invalid rows are filtered out of the stream without creating a quarantine table or failing pipeline. | Feed 100 transaction records where 10 have `amount <= 0` under rule `{"expression": "amount > 0", "action": "drop"}`. Target table must store only the 90 valid rows. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `bronze_txns`<br>• **Target Table**: `txns_clean_raw`<br>• **Landing Volume**: `/Volumes/{{catalog}}/txns/landing_dq_drop/` | • **Pipeline**: `metaflow_test_dq_002_drop_pipeline`<br>• **Job**: `metaflow_test_dq_002_drop_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT count(*) FROM {{catalog}}.bronze_txns.txns_clean_raw;` → 90.<br>`information_schema.tables` contains NO `txns_clean_raw_quarantine` table. |
| **TC-DQ-003** | **DQ Action: `fail` Pipeline Execution Abort**<br>Verify critical rule failure immediately halts Lakeflow pipeline update and alerts workflow job. | Feed batch containing a null primary key under rule `{"expression": "account_id IS NOT NULL", "action": "fail"}`. Lakeflow pipeline must fail immediately. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `bronze_banking`<br>• **Target Table**: `accounts_strict_raw`<br>• **Landing Volume**: `/Volumes/{{catalog}}/banking/landing_dq_fail/` | • **Pipeline**: `metaflow_test_dq_003_fail_pipeline`<br>• **Job**: `metaflow_test_dq_003_fail_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **Execution Assertion**:<br>Pipeline update status returns `FAILED`.<br>Task 1 in workflow job fails with `ExpectationViolationException`. |
| **TC-DQ-004** | **DQ Action: `quarantine` Routing & Diagnostics**<br>Verify invalid records route to companion `<table>_quarantine` enriched with rule violation diagnostics. | Feed 100 customer records: 85 valid, 10 with null `Country`, 5 with invalid `Email`.<br>Main table `customer_raw` gets 85 rows.<br>`customer_raw_quarantine` gets 15 rows with diagnostic columns populated. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `bronze_test`<br>• **Target Table**: `customer_raw`<br>• **Quarantine Table**: `customer_raw_quarantine`<br>• **Landing Volume**: `/Volumes/{{catalog}}/test/landing_dq_quarantine/` | • **Pipeline**: `metaflow_test_dq_004_quarantine_pipeline`<br>• **Job**: `metaflow_test_dq_004_quarantine_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT count(*) FROM {{catalog}}.bronze_test.customer_raw;` → 85.<br>`SELECT count(*) FROM {{catalog}}.bronze_test.customer_raw_quarantine;` → 15.<br>`SELECT __framework_dq_failed_rule_ids, __framework_record_id FROM {{catalog}}.bronze_test.customer_raw_quarantine;` contains exact failed rule IDs. |

---

### Module 5: Cryptography, Key Management & Secrets

Tests AES-GCM column encryption, Unity Catalog secret resolution, SHA-256 hash generation, and secret masking.

| Test Case No | Testing Functionality & Objective | Realistic Production Scenario | Databricks Object Names | Proposed Pipeline & Job Architecture | Expected Output & Verification Logic |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **TC-SEC-001** | **Column-Level AES-GCM Encryption with Random IV**<br>Verify sensitive PII columns are encrypted with AES-GCM before writing to storage. | Ingest customer data with PII columns `ssn` and `credit_card`. Spec defines `target_config.encrypted_columns: ["ssn", "credit_card"]`. Underlying Delta Parquet files must store ciphertext. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `bronze_customers`<br>• **Target Table**: `customer_pii_encrypted`<br>• **Secret Scope**: `metaflow_sec_scope`<br>• **Secret Key**: `aes_gcm_256_key`<br>• **Landing Volume**: `/Volumes/{{catalog}}/customers/landing_pii/` | • **Pipeline**: `metaflow_test_sec_001_aes_encrypt_pipeline`<br>• **Job**: `metaflow_test_sec_001_aes_encrypt_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT ssn, credit_card FROM {{catalog}}.bronze_customers.customer_pii_encrypted;` returns non-plaintext binary/base64 strings.<br>Decrypting with secret key recovers original plaintext. |
| **TC-SEC-002** | **SHA-256 Hash Key & Value Determinism**<br>Verify `__framework_hash_key` and `__framework_hash_value` compute deterministically for downstream reconciliation. | Ingest rows with composite primary key `(region, account_id)`. Framework generates SHA-256 hash key across PKs and hash value across comparison columns. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `silver_accounts`<br>• **Target Table**: `accounts_hashed`<br>• **Landing Volume**: `/Volumes/{{catalog}}/accounts/landing_hash/` | • **Pipeline**: `metaflow_test_sec_002_hashing_pipeline`<br>• **Job**: `metaflow_test_sec_002_hashing_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT __framework_hash_key, sha2(concat_ws('||', region, account_id), 256) FROM {{catalog}}.silver_accounts.accounts_hashed;` values match exactly for 100% of rows. |
| **TC-SEC-003** | **Secret Masking & Redaction in Telemetry**<br>Verify encryption keys retrieved via `dbutils.secrets.get()` are completely redacted from driver logs and OTEL events. | Pipeline executes AES encryption and PGP sink packaging. Inspect all driver stdout, execution plans, and OTEL log events. Secret strings must never appear in plaintext. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `bronze_sec`<br>• **Target Table**: `secrets_audit_raw` | • **Pipeline**: `metaflow_test_sec_003_redaction_pipeline`<br>• **Job**: `metaflow_test_sec_003_redaction_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **Log Inspection Assertion**:<br>Grep driver and task logs for secret key string → 0 matches.<br>All secret references display as `[REDACTED]`. |

---

### Module 6: Governance & Metadata Tagging

Tests post-deployment automated Unity Catalog table and column tagging via DDL tasks.

| Test Case No | Testing Functionality & Objective | Realistic Production Scenario | Databricks Object Names | Proposed Pipeline & Job Architecture | Expected Output & Verification Logic |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **TC-GOV-001** | **Post-Deployment UC Table & Column Tag Application**<br>Verify automated execution of `ALTER TABLE ... SET TAGS` and `ALTER COLUMN ... SET TAGS`. | Spec defines table tags `{"domain": "finance", "compliance": "sox"}` and column tags `{"CustomerID": {"classification": "restricted", "mask": "PII"}}`. Post-pipeline task applies tags to materialized table. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `bronze_crm`<br>• **Target Table**: `customer_raw`<br>• **Tag Task Notebook**: `notebooks/04_governance/04_apply_governance_and_egress.py` | • **Pipeline**: `metaflow_test_gov_001_tagging_pipeline`<br>• **Job**: `metaflow_test_gov_001_tagging_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT tag_name, tag_value FROM system.information_schema.table_tags WHERE table_name = 'customer_raw';` → contains `domain: finance`, `compliance: sox`.<br>`column_tags` contains `classification: restricted`. |
| **TC-GOV-002** | **Governance Tag Application Idempotency**<br>Verify re-running tag application DDL against existing tags does not error or cause metadata lock conflicts. | Re-run `04_apply_governance_and_egress.py` three times in succession against existing table. Tag application must succeed without DDL conflicts. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `bronze_crm`<br>• **Target Table**: `customer_raw` | • **Pipeline**: `metaflow_test_gov_002_idempotent_tag_pipeline`<br>• **Job**: `metaflow_test_gov_002_idempotent_tag_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **Execution Assertion**:<br>All 3 runs return exit code 0.<br>Tags remain active and uncorrupted in Unity Catalog. |

---

### Module 7: Cross-Dataset Reconciliation & Self-Healing

Tests dataset comparison (`source_to_target`, `target_to_source`, `both`), value drift detection, hash optimization, and self-healing backfills to source bus tables.

| Test Case No | Testing Functionality & Objective | Realistic Production Scenario | Databricks Object Names | Proposed Pipeline & Job Architecture | Expected Output & Verification Logic |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **TC-REC-001** | **Missing Target Record Detection & Self-Healing Backfill**<br>Detect records missing from target Delta table and self-heal by appending back into source bus table. | Batch source `autoload_bronze` has 12 records (`C001`–`C012`). Streaming target `zerobus_bronze` only has 10 records (`C001`–`C010`). Recon engine detects missing `C011`/`C012` and appends them into `zerobus_source_bus`. | • **Catalog**: `{{catalog}}`<br>• **Source Table**: `{{catalog}}.bronze_excalibur.autoload_bronze`<br>• **Target Table**: `{{catalog}}.bronze_excalibur.zerobus_bronze`<br>• **Self-Heal Bus Table**: `{{catalog}}.excalibur_usecase.zerobus_source_bus`<br>• **Recon Spec ID**: `recon_autoload_vs_zerobus` | • **Pipeline**: `metaflow_test_rec_001_selfheal_pipeline`<br>• **Job**: `metaflow_test_rec_001_selfheal_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT records_missing_in_target FROM {{catalog}}.config.reconciliation_run_log WHERE reconciliation_id = 'recon_autoload_vs_zerobus';` → 2.<br>`SELECT count(*) FROM {{catalog}}.excalibur_usecase.zerobus_source_bus WHERE customer_id IN ('C011', 'C012');` → 2. |
| **TC-REC-002** | **Attribute Value Drift Detection (`VALUE_DRIFT`)**<br>Detect records where primary keys match but comparison column attributes have drifted. | Baseline table and CDC target both contain customer `C001`. In source, `status = 'SUSPENDED'`; in target, `status = 'ACTIVE'`. Recon engine logs per-attribute value drift without mutating target. | • **Catalog**: `{{catalog}}`<br>• **Baseline Table**: `{{catalog}}.silver_crm.customer_baseline`<br>• **Target Table**: `{{catalog}}.silver_crm.customer_cdc`<br>• **Mismatch Table**: `{{catalog}}.config.reconciliation_mismatch_log` | • **Pipeline**: `metaflow_test_rec_002_drift_pipeline`<br>• **Job**: `metaflow_test_rec_002_drift_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT diff_type, source_value, target_value FROM {{catalog}}.config.reconciliation_mismatch_log WHERE record_key = 'C001';` → `VALUE_DRIFT`, `SUSPENDED`, `ACTIVE`. |
| **TC-REC-003** | **Pre-Computed Hash Matcher Performance Optimization**<br>Verify `hash_precomputed: true` joins directly on `__framework_hash_key` and checks drift on `__framework_hash_value`. | Large datasets (1M rows) compared. With `hash_precomputed: true`, Spark query plan performs single-column integer/binary join on pre-computed hashes instead of evaluating multi-column hash expressions. | • **Catalog**: `{{catalog}}`<br>• **Source Table**: `{{catalog}}.silver_sales.orders_src`<br>• **Target Table**: `{{catalog}}.silver_sales.orders_tgt` | • **Pipeline**: `metaflow_test_rec_003_precomputed_hash_pipeline`<br>• **Job**: `metaflow_test_rec_003_precomputed_hash_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **Plan Assertion**:<br>Spark `EXPLAIN` shows physical join condition `orders_src.__framework_hash_key = orders_tgt.__framework_hash_key`. Execution completes within SLA. |

---

### Module 8: External Egress & Sinks

Tests target types `sink` (pure egress export) and `external_sink` (materialize queryable Delta table + egress export) across Delta, Kafka, and PGP-ZIP formats.

| Test Case No | Testing Functionality & Objective | Realistic Production Scenario | Databricks Object Names | Proposed Pipeline & Job Architecture | Expected Output & Verification Logic |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **TC-SNK-001** | **External Sink Dual Materialization & Plain ZIP Export**<br>Verify `target_type: "external_sink"` materializes a queryable Delta table AND exports a `.zip` archive to egress Volume. | 4-way join dataset materializes table `silver_ea.ea_joined_assignments` AND packages joined rows as plain `ea_joined_export_<batch_id>.zip` in `/Volumes/{{catalog}}/egress_ea/export_zips/output/`. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `silver_ea`<br>• **Target Table**: `ea_joined_assignments`<br>• **Egress Volume**: `/Volumes/{{catalog}}/egress_ea/export_zips/output/`<br>• **Staging Volume**: `/Volumes/{{catalog}}/egress_ea/export_zips/_staging/` | • **Pipeline**: `metaflow_test_snk_001_external_sink_pipeline`<br>• **Job**: `metaflow_test_snk_001_external_sink_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **Verification**:<br>`SELECT count(*) FROM {{catalog}}.silver_ea.ea_joined_assignments;` → 5.<br>Volume contains valid unencrypted `.zip` file containing exact 5 CSV rows. |
| **TC-SNK-002** | **Pure Sink Export Without Table Materialization (`sink`)**<br>Verify `target_type: "sink"` writes streaming records directly to an egress Volume without registering a UC table. | Real-time sensor feed streamed directly to partner egress Volume. No Delta table created in Unity Catalog to eliminate storage/governance overhead. | • **Catalog**: `{{catalog}}`<br>• **Egress Volume**: `/Volumes/{{catalog}}/egress_iot/partner_drops/` | • **Pipeline**: `metaflow_test_snk_002_pure_sink_pipeline`<br>• **Job**: `metaflow_test_snk_002_pure_sink_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **Verification**:<br>Egress Volume receives exported data files.<br>`information_schema.tables` contains NO table for this sink flow. |
| **TC-SNK-003** | **PGP Encrypted ZIP Sink Export with Secret Key**<br>Verify sink with `pgp_encryption.enabled: true` generates an armored `.zip.pgp` file encrypted with partner public key. | High-security financial extract must be exported as an encrypted `.zip.pgp` archive into egress Volume using public key from Databricks Secrets. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `silver_finance`<br>• **Target Table**: `monthly_settlements`<br>• **Egress Volume**: `/Volumes/{{catalog}}/finance_egress/secure_drops/`<br>• **Secret Scope**: `metaflow_partner_scope`<br>• **Secret Key**: `partner_pgp_pubkey` | • **Pipeline**: `metaflow_test_snk_003_pgp_zip_sink_pipeline`<br>• **Job**: `metaflow_test_snk_003_pgp_zip_sink_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **Verification**:<br>Exported file carries `.zip.pgp` extension in Volume.<br>Decrypting file with matching private key yields valid `.zip` containing settlement records. |

---

### Module 9: Observability & Telemetry Framework

Tests structured JSON logging, DLT event log extraction, OpenTelemetry (OTEL) payload creation, and dispatch to UC Volume (`.jsonl.gz`) and OTLP collectors.

| Test Case No | Testing Functionality & Objective | Realistic Production Scenario | Databricks Object Names | Proposed Pipeline & Job Architecture | Expected Output & Verification Logic |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **TC-OBS-001** | **In-Pipeline Structured JSON Logging Validation**<br>Verify all pipeline steps emit valid JSON structured logs with mandatory execution context. | Pipeline executes ingestion, DQ evaluation, and CDC dispatch. Standard output captures structured JSON logs with `flow_id`, `step`, `duration_ms`, `records_read`, `records_written`. | • **Catalog**: `{{catalog}}`<br>• **Target Table**: `test_structured_log_raw` | • **Pipeline**: `metaflow_test_obs_001_json_log_pipeline`<br>• **Job**: `metaflow_test_obs_001_json_log_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **Log Assertion**:<br>Parse all stdout logs with JSON parser. Assert 100% of framework logs are valid JSON with non-null `timestamp_utc` and `pipeline_run_id`. |
| **TC-OBS-002** | **DLT Event Log Extraction & OpenTelemetry Formatting**<br>Verify Observability Engine reads DLT event log and formats events into standard OpenTelemetry (OTEL) JSON schema. | Task 2 (`run_observability_task`) reads event log from Lakeflow pipeline storage and constructs OTEL `ResourceSpans` and `LogRecord` objects. | • **Catalog**: `{{catalog}}`<br>• **Observability Config Table**: `{{catalog}}.config.observability_config`<br>• **Pipeline ID**: `${resources.pipelines.metaflow_test_obs_002_pipeline.id}` | • **Pipeline**: `metaflow_test_obs_002_otel_build_pipeline`<br>• **Job**: `metaflow_test_obs_002_otel_build_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **Assertion**:<br>Validate generated OTEL JSON payload against official OpenTelemetry Log/Trace schema specification using JSON Schema Validator. |
| **TC-OBS-003** | **Telemetry Export to Unity Catalog Volume (`.jsonl.gz`)**<br>Verify compressed GZIP JSONL telemetry files are written to destination UC Volume path. | Spec configures destination `DATABRICKS_VOLUME` pointing to `/Volumes/{{catalog}}/observability/app_logs/`. Task 2 writes compressed `.jsonl.gz` logs. | • **Catalog**: `{{catalog}}`<br>• **Telemetry Volume**: `/Volumes/{{catalog}}/observability/app_logs/` | • **Pipeline**: `metaflow_test_obs_003_vol_export_pipeline`<br>• **Job**: `metaflow_test_obs_003_vol_export_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **Volume Verification**:<br>Decompress `.jsonl.gz` from volume path. Verify records match Lakeflow pipeline run metrics and expectation failure counts. |

---

### Module 10: Fault Tolerance, Schema Evolution & Recovery

Tests idempotent re-execution, recovery after simulated mid-stream crashes, and continuous Day-1 to Day-2 schema migrations.

| Test Case No | Testing Functionality & Objective | Realistic Production Scenario | Databricks Object Names | Proposed Pipeline & Job Architecture | Expected Output & Verification Logic |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **TC-FLT-001** | **Pipeline Mid-Stream Crash Recovery & Checkpoint Integrity**<br>Verify Lakeflow pipeline resumes seamlessly from checkpoint after simulated node/cluster failure. | Abort pipeline update mid-stream during micro-batch execution. Trigger update again via Workflow Job. Lakeflow engine reads checkpoint and completes with zero duplicate rows. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `bronze_resilience`<br>• **Target Table**: `crash_test_raw`<br>• **Checkpoint**: `/Volumes/{{catalog}}/resilience/_checkpoints/crash_test/` | • **Pipeline**: `metaflow_test_flt_001_recovery_pipeline`<br>• **Job**: `metaflow_test_flt_001_recovery_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT count(*) FROM {{catalog}}.bronze_resilience.crash_test_raw;` equals exact source record count. Zero duplicates. |
| **TC-FLT-002** | **Continuous Day-1 to Day-2 Incremental Ingestion Cycle**<br>Verify seamless transition from initial seed load to daily incremental delta loads. | Day-1: Ingest initial seed of 1,000 customers.<br>Day-2: Ingest 150 customer updates and 50 new customer inserts.<br>SCD1 table updates existing rows and inserts new rows seamlessly. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `silver_crm`<br>• **Target Table**: `dim_customer_lifecycle`<br>• **Landing Volume**: `/Volumes/{{catalog}}/crm/landing_lifecycle/` | • **Pipeline**: `metaflow_test_flt_002_lifecycle_pipeline`<br>• **Job**: `metaflow_test_flt_002_lifecycle_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT count(*) FROM {{catalog}}.silver_crm.dim_customer_lifecycle;` → exactly 1,050 rows.<br>Updated customers reflect Day-2 attribute values. |

---

### Module 11: Dynamic Parameterization & Templating Engine (All Components)

Tests dynamic runtime parameter substitution (`${param}`) and template resolution (`{{catalog}}`, `{{env}}`) across file paths, ZIP handling, transformation SQL, reconciliation filter conditions, self-healing projection logic, DQ rule expressions, and egress sink file formats.

| Test Case No | Testing Functionality & Objective | Realistic Production Scenario | Databricks Object Names | Proposed Pipeline & Job Architecture | Expected Output & Verification Logic |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **TC-PRM-001** | **Parameterized Dynamic Volume & File Paths**<br>Verify dynamic resolution of `${data_domain}` and `${batch_date}` tokens in Auto Loader and ZIP landing paths. | Spec configures `pipeline_parameters: {"data_domain": "finance_ops", "batch_date": "2026-08-28"}`.<br>Volume paths in spec define:<br>`path: "/Volumes/{{catalog}}/${data_domain}/landing/${batch_date}/"` and `source_zip_path: "/Volumes/{{catalog}}/${data_domain}/zips/${batch_date}/"`.<br>Framework substitutes tokens before Auto Loader discovers files. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `bronze_finance`<br>• **Target Table**: `daily_txns_raw`<br>• **Landing Volume**: `/Volumes/{{catalog}}/finance_ops/landing/2026-08-28/`<br>• **ZIP Volume**: `/Volumes/{{catalog}}/finance_ops/zips/2026-08-28/` | • **Pipeline**: `metaflow_test_prm_001_path_param_pipeline`<br>• **Job**: `metaflow_test_prm_001_path_param_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT count(*) FROM {{catalog}}.bronze_finance.daily_txns_raw;` equals file row count.<br>`SELECT DISTINCT __framework_source_file_name FROM {{catalog}}.bronze_finance.daily_txns_raw;` contains `/Volumes/{{catalog}}/finance_ops/landing/2026-08-28/`. |
| **TC-PRM-002** | **Parameterized Transformation SQL Filters & Auto-Quoting**<br>Verify `${param}` substitution in `transformation_sql` correctly auto-quotes strings and leaves numeric/boolean literals unquoted. | Spec defines `pipeline_parameters: {"filter_country": "US", "min_amount": 250.50, "is_vip": true}`.<br>Transformation SQL uses `WHERE country = ${filter_country} AND amount >= ${min_amount} AND vip_flag = ${is_vip}`.<br>Substitution produces `WHERE country = 'US' AND amount >= 250.50 AND vip_flag = True` (no manual quoting needed in spec). | • **Catalog**: `{{catalog}}`<br>• **Source Table**: `{{catalog}}.bronze_sales.orders_raw`<br>• **Target Table**: `{{catalog}}.silver_sales.us_vip_orders` | • **Pipeline**: `metaflow_test_prm_002_sql_param_pipeline`<br>• **Job**: `metaflow_test_prm_002_sql_param_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT count(*) FROM {{catalog}}.silver_sales.us_vip_orders WHERE country != 'US' OR amount < 250.50 OR vip_flag != true;` → 0 rows.<br>Query plan verifies exact literal substitution without Spark SQL parse errors. |
| **TC-PRM-003** | **Parameterized Reconciliation `filter_condition` & `transform_sql`**<br>Verify dynamic parameter substitution in reconciliation dataset filtering and self-healing projection logic. | Spec defines `pipeline_parameters: {"recon_partition": "2026-08", "region_code": "EMEA", "audit_user": "RECON_BOT"}`.<br>Reconciliation spec uses:<br>• `source_config.filter_condition`: `partition_month = ${recon_partition} AND region = ${region_code}`<br>• `transform_sql`: `SELECT customer_id, customer_name, status, ${audit_user} AS modified_by FROM _reconciliation_unmatched_records`. | • **Catalog**: `{{catalog}}`<br>• **Source Table**: `{{catalog}}.silver_crm.crm_source`<br>• **Target Table**: `{{catalog}}.silver_crm.crm_target`<br>• **Heal Bus Table**: `{{catalog}}.crm_bus.customer_bus`<br>• **Recon ID**: `recon_prm_crm_sync` | • **Pipeline**: `metaflow_test_prm_003_recon_param_pipeline`<br>• **Job**: `metaflow_test_prm_003_recon_param_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>Reconciliation only scans `EMEA` records for `2026-08`.<br>`SELECT DISTINCT modified_by FROM {{catalog}}.crm_bus.customer_bus WHERE customer_id IN (SELECT record_key FROM {{catalog}}.config.reconciliation_mismatch_log);` → returns `'RECON_BOT'`. |
| **TC-PRM-004** | **Parameterized Data Quality (DQ) Rule Expressions**<br>Verify dynamic parameter substitution inside DQ rule expectation expressions. | Spec defines `pipeline_parameters: {"max_allowed_txn_limit": 50000, "cutoff_date": "2026-01-01"}`.<br>DQ rule defines `{"rule_id": "valid_txn_bounds", "expression": "txn_amount <= ${max_allowed_txn_limit} AND txn_date >= '${cutoff_date}'", "action": "quarantine"}`.<br>Rows exceeding 50,000 are quarantined. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `bronze_txns`<br>• **Target Table**: `txns_validated`<br>• **Quarantine Table**: `txns_validated_quarantine` | • **Pipeline**: `metaflow_test_prm_004_dq_param_pipeline`<br>• **Job**: `metaflow_test_prm_004_dq_param_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **SQL Assertion**:<br>`SELECT max(txn_amount) FROM {{catalog}}.bronze_txns.txns_validated;` <= 50,000.<br>`SELECT min(txn_amount) FROM {{catalog}}.bronze_txns.txns_validated_quarantine WHERE __framework_dq_failed_rule_ids LIKE '%valid_txn_bounds%';` > 50,000. |
| **TC-PRM-005** | **Parameterized Egress Sink Paths & Export File Naming**<br>Verify dynamic substitution in egress output directory and exported archive file naming convention. | Spec defines `pipeline_parameters: {"client_code": "ACME_CORP", "export_tier": "GOLD"}`.<br>Sink config specifies:<br>• `output_zip_path`: `"/Volumes/{{catalog}}/egress/${client_code}/output/"`<br>• `export_file_name_format`: `"${client_code}_${export_tier}_export_{batch_id}"`. | • **Catalog**: `{{catalog}}`<br>• **Schema**: `silver_exports`<br>• **Target Table**: `acme_export_staging`<br>• **Egress Volume**: `/Volumes/{{catalog}}/egress/ACME_CORP/output/` | • **Pipeline**: `metaflow_test_prm_005_sink_param_pipeline`<br>• **Job**: `metaflow_test_prm_005_sink_param_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **Volume Verification**:<br>Output directory `/Volumes/{{catalog}}/egress/ACME_CORP/output/` contains `.zip` archive matching pattern `ACME_CORP_GOLD_export_*.zip`. |
| **TC-PRM-006** | **Negative Validation: Undefined Parameter Reference Error**<br>Verify pipeline validation halts with explicit error when SQL references an undefined `${missing_param}`. | Transformation SQL contains `WHERE department_id = ${dept_code}`, but `pipeline_parameters` omits `dept_code`. Onboarding preflight must reject spec immediately with `FrameworkConfigError`. | • **Catalog**: `{{catalog}}`<br>• **Target Table**: `invalid_param_table`<br>• **Error Class**: `FrameworkConfigError` | • **Pipeline**: `metaflow_test_prm_006_negative_param_pipeline`<br>• **Job**: `metaflow_test_prm_006_negative_param_job`<br>• **Task 1**: `run_pipeline_task`<br>• **Task 2**: `run_observability_task` | **Validation Assertion**:<br>`validate_spec()` raises error: `"Transformation SQL references undefined parameter(s): ['dept_code']"`.<br>Zero records committed to `{catalog}.config.transformation_flow_spec`. |

---

## 4. Master Test Suite Execution Runbook

### Step 1: Build & Deploy Databricks Asset Bundle (DAB)
Deploy all pipelines, workflow jobs, notebooks, and wheel artifacts to the target workspace:
```bash
# Authenticate with Databricks CLI
databricks auth login --host https://<databricks-instance>

# Deploy Asset Bundle to dev target
databricks bundle deploy --target dev
```

### Step 2: Trigger Dedicated 2-Task Workflow Jobs
Run individual test scenario workflow jobs (each executing `run_pipeline_task` followed by `run_observability_task`):

```bash
# Ingestion Test 001: Selective ZIP Glob Pattern Extraction
databricks bundle run metaflow_test_ing_001_zip_filter_job --target dev

# CDC Test 003: SCD1 Overwrite with Delete Mapping
databricks bundle run metaflow_test_cdc_003_scd1_job --target dev

# Parameterization Test 001: Parameterized Dynamic Paths
databricks bundle run metaflow_test_prm_001_path_param_job --target dev

# Parameterization Test 002: Parameterized Transformation SQL
databricks bundle run metaflow_test_prm_002_sql_param_job --target dev

# Parameterization Test 003: Parameterized Reconciliation & Self-Healing
databricks bundle run metaflow_test_prm_003_recon_param_job --target dev

# Data Quality Test 004: Quarantine Routing & Diagnostic Enrichment
databricks bundle run metaflow_test_dq_004_quarantine_job --target dev

# Reconciliation Test 001: Cross-Dataset Self-Healing Backfill
databricks bundle run metaflow_test_rec_001_selfheal_job --target dev

# Sink Test 001: External Sink Dual Materialization & ZIP Export
databricks bundle run metaflow_test_snk_001_external_sink_job --target dev
```

### Step 3: Run Automated Pytest Assertion Suite
Execute automated Python assertion suites against the materialized Unity Catalog objects:
```bash
# Run unit assertions
pytest tests/unit/ -v

# Run post-deployment integration assertions
pytest tests/integration/test_metaflow_001_zip_join_export.py -v
pytest tests/integration/test_metaflow_002_zerobus_data_load.py -v
pytest tests/integration/test_metaflow_003_autoload_recon_append.py -v
```

---

*Document maintained for the NextGen Metadata Framework (Metaflow). For technical architecture details, refer to [`docv2/04_technical_architecture.md`](../docv2/04_technical_architecture.md).*
