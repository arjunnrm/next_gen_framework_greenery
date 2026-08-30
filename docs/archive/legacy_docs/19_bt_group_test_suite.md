# BT_Group Test Suite: UC001-UC005

> See also: [README.md](README.md) — the full Metaflow documentation set.

## Purpose

Five production-grade acceptance-test pipelines, each proving a distinct Metaflow engine
capability, all onboarded through configuration alone against a dedicated `BT_Group` Unity
Catalog catalog (separate from this repo's `poc` demo catalog). One new, small, justified
engine capability was added to support UC002 (`source_zip_handling` generalized from
`asn1`-only to also work with `autoloader` -- see
[02_cdc_load_strategies.md](02_cdc_load_strategies.md) and
`ingestion/readers.py::_apply_source_zip_handling`); every other use case runs entirely on
engine capabilities that already existed before this doc was written.

**Control metadata still lives in the control catalog** (`${var.catalog}`, `poc` by
default) via the existing `01_setup_control_tables`/`02_onboarding_engine` notebooks --
BT_Group holds only the *data*. This is the same separation already used throughout this
repo: a flow's `target_catalog` has never had to match the catalog its own control-table
row lives in.

| UC | Name | Spec | Pipeline | New engine code? |
|---|---|---|---|---|
| UC001 | Zerobus Multi-Table CDC Ingestion | `spec_15_bt_uc001_zerobus_multitable_cdc.json` | `uc001_zerobus_multitable_cdc_pipeline` | No |
| UC002 | Archive/Auto Loader Outbound Export | `spec_16_bt_uc002_archive_autoloader_egress.json` | `uc002_archive_autoloader_egress_pipeline` | Yes -- `source_zip_handling` for `autoloader` |
| UC003 | ASN.1 Decode + DQ + Quarantine | `spec_17_bt_uc003_asn1_dq_quarantine.json` | `uc003_asn1_dq_quarantine_pipeline` | No |
| UC004 | Intra-Pipeline Reconciliation Drift Audit | `spec_18_bt_uc004_reconciliation_drift_audit.json` | `uc004_reconciliation_drift_audit_pipeline` | No |
| UC005 | Schema Evolution + Error Handling | `spec_19_bt_uc005_schema_evolution.json` | `uc005_schema_evolution_pipeline` | No |

---

## UC001: Zerobus Multi-Table CDC Ingestion

### Objective

Validate multi-table streaming ingestion and CDC patterns simulating Zerobus payloads --
one CDC strategy per table, all as **ingestion-level** CDC (no transformation layer
needed; `SCD1`/`SCD2`/`APPEND` are all natively supported at ingestion, see
[02_cdc_load_strategies.md](02_cdc_load_strategies.md)).

### Schema

* Producer tables (simulated Zerobus feeds), `BT_Group.zerobus_sources`:
  * `src_product_catalog`: `product_id` (PK), `product_name`, `category`, `unit_price`,
    `status`, `updated_at`.
  * `src_customer_master`: `customer_id` (PK), `customer_name`, `segment`, `region`,
    `tier`, `updated_at`.
  * `src_order_events`: `event_id`, `order_id`, `customer_id`, `product_id`, `quantity`,
    `event_ts`, `event_type`.
* Bronze targets, `BT_Group.bronze_zerobus`:
  * `raw_product_catalog_scd1` -- `cdc_load_strategy: SCD1`, keys `product_id`.
  * `raw_customer_master_scd2` -- `cdc_load_strategy: SCD2`, keys `customer_id`,
    `columns_to_check: [segment, tier]` (region is intentionally untracked, proving
    selective history scope -- same pattern as [08_test_pipeline_1_volume_scd.md](08_test_pipeline_1_volume_scd.md)).
  * `raw_order_events_append` -- `cdc_load_strategy: APPEND`, no keys.

### Main execution flow

1. `resources/bt_group_test_suite_job.yml`: `setup_control_tables` -> `seed_bt_group_data`
   -> `onboard_spec_15` -> `run_uc001_zerobus_multitable_cdc` -> `apply_governance_uc001`.
2. `notebooks/00_seed_sample_data/01_seed_bt_group_data.py` seeds all 3 producer tables
   from `_batch1.csv` (idempotent MERGE on natural key).
3. First run: `raw_product_catalog_scd1` has 5 rows (1/product), `raw_customer_master_scd2`
   has 5 rows (1 version/customer), `raw_order_events_append` has 6 rows.
4. Day 2: re-run `seed_zerobus_style_table_from_csv("uc001_src_product_catalog_batch2.csv",
   ...)` (and the customer/order equivalents) from the seeding notebook, then re-run the
   pipeline: `PC001`'s price overwrites in place, `PC006` inserts; `CM002`'s tier change
   opens a second SCD2 version, `CM003`'s region-only change does **not** (region isn't
   tracked), `CM006` inserts; 2 new order events append.

### Validation queries

```sql
-- SCD1: exactly one row per product, latest price wins
SELECT product_id, unit_price, updated_at FROM BT_Group.bronze_zerobus.raw_product_catalog_scd1 ORDER BY product_id;
SELECT COUNT(*) AS rows, COUNT(DISTINCT product_id) AS distinct_products FROM BT_Group.bronze_zerobus.raw_product_catalog_scd1;

-- SCD2: two versions for CM002 (tier changed), one for CM003 (only region changed)
SELECT customer_id, tier, region, __START_AT, __END_AT
FROM BT_Group.bronze_zerobus.raw_customer_master_scd2
WHERE customer_id IN ('CM002', 'CM003') ORDER BY customer_id, __START_AT;

-- Append: monotonically growing, never overwritten
SELECT COUNT(*) FROM BT_Group.bronze_zerobus.raw_order_events_append;
```

### Design decisions

* **Ingestion-level CDC, not a transformation layer** -- `SCD1`/`SCD2` are natively
  supported at ingestion (`ALLOWED_INGESTION_CDC_STRATEGIES` includes both), so this use
  case needs zero transformation flows. Adding a transformation layer here would have been
  unnecessary indirection for a requirement that's purely about *ingestion* patterns.

---

## UC002: Archive/Auto Loader Outbound Export

### Objective

Unzip ->
[Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
ingest -> multi-table join -> streaming ZIP export, **entirely within one Lakeflow
Declarative Pipeline update** -- no separate job task for any step.

### The one new engine capability

`ingestion/readers.py::_apply_source_zip_handling` previously only ran inside
`read_asn1_source`. It's now called from `read_autoloader_source` too (same function,
same `source_zip_handling` config shape, now validated for both `source_type`s in
`spec_validator.py::_validate_source_zip_handling`). Because the reader function runs at
the pipeline's actual *execution* time (Lakeflow re-invokes every `@dlt.table` function
body on each update), the unzip step and the Auto Loader read happen inside the same
pipeline update as everything downstream of them.

The silver export's `external_sink` egress described below is a second example of this
same "entirely inside one pipeline update" property, for a related reason -- `target_type:
"external_sink"` is registered via
`engine/sink_registration.py::register_external_sink_export`, a genuine
`dlt.create_sink`/`@dlt.append_flow` pair defined at graph-definition time like any other
flow output -- see [15_engine_refactor.md](15_engine_refactor.md) and
[23_lakeflow_sinks.md](23_lakeflow_sinks.md). So the objective's "no separate job task for
any step" is literally true end to end, including the egress write itself.

### Schema

* 4 ZIP archives -> 4 Bronze tables, `BT_Group.bronze_archive_egress`:
  `raw_sales_north` / `raw_sales_south` (fact: `order_id`, `region`, `product_id`,
  `customer_id`, `quantity`), `raw_ref_customers` (`customer_id`, `customer_name`,
  `tier`), `raw_ref_products` (`product_id`, `product_name`, `unit_price`).
* Silver export, `BT_Group.silver_archive_egress.unified_sales_export`
  (`target_type: external_sink`): the two sales tables (streaming) `UNION ALL`'d, then
  joined against the two reference tables (read as **batch**, not streaming -- see Design
  decisions) to compute `line_total`. A real, governed, DQ-quarantined table is
  materialized (unchanged CDC dispatch, `cdc_load_strategy: APPEND`), exactly like every
  other `target_type` -- see [01_control_metadata_schema.md](01_control_metadata_schema.md)
  §4.
* Egress: `target_config.sink_config` is `{"format": "pgp_zip", "path":
  "/Volumes/BT_Group/egress/uc002_export/csv/_staging/", "post_export_archive":
  {"enabled": true, "output_zip_path": "/Volumes/BT_Group/egress/uc002_export/zips/unified_sales_export/"}}`
  -- a second `@dlt.append_flow` reads the now-materialized `unified_sales_export` table
  and writes it through the genuine custom `pgp_zip` Lakeflow sink
  (`archive/pgp_zip_sink.py`), which stages each streaming micro-batch's rows under `path`
  and archives them into `output_zip_path`. No `post_export_archive.secret`/
  `pgp_encryption` is configured here, so the archive is a plain, unencrypted ZIP (see
  Design decisions).

### Main execution flow

1. `notebooks/00_seed_sample_data/01_seed_bt_group_data.py` generates all 4 ZIPs directly
   on-cluster (workspace sync silently mangles pre-built `.zip` uploads -- same reasoning
   as `00_seed_sample_data.py`'s existing ZIP/ASN.1 fixtures).
2. `onboard_spec_16` -> `run_uc002_archive_autoloader_egress`: each of the 4 ingestion
   flows extracts its own ZIP into its own subfolder, then Auto Loader reads that
   subfolder -- all before the silver join even starts.
3. The same pipeline update's silver transformation flow joins all 4, materializes
   `unified_sales_export`, and registers the `pgp_zip` sink export -- both the table and
   its ZIP export commit as part of this one `run_uc002_archive_autoloader_egress`
   pipeline update. `apply_governance_uc002` (governance tags only -- there is no
   egress step left for it to run) runs afterward purely to apply `governance_tags`.

### Validation queries

```sql
SELECT COUNT(*) FROM BT_Group.bronze_archive_egress.raw_sales_north;
SELECT COUNT(*) FROM BT_Group.bronze_archive_egress.raw_sales_south;
SELECT * FROM BT_Group.silver_archive_egress.unified_sales_export ORDER BY order_id;
```

```bash
databricks fs ls "dbfs:/Volumes/BT_Group/egress/uc002_export/zips/" --profile dev
databricks fs ls "dbfs:/Volumes/BT_Group/egress/uc002_export/csv/" --profile dev
```

### Design decisions

* **Reference tables joined as batch, not stream-stream.** `raw_ref_customers`/
  `raw_ref_products` are small, slowly-changing dimension lookups -- joining them as a
  genuine stream-stream join would require watermarks on all 4 inputs for no real benefit.
  `is_streaming: false` on the two reference `source_inputs` makes this a stream-static
  join instead (no watermark requirement), which is both simpler and the architecturally
  correct choice for dimension data. The two sales tables *are* streamed (`UNION ALL`'d),
  since they're the genuine incremental fact data.
* **No archive password configured** -- `post_export_archive.secret` (a
  [Unity Catalog secret](https://docs.databricks.com/aws/en/security/secrets/)
  `{secret_catalog, secret_schema, secret_key}` reference) is omitted, producing a plain
  (unencrypted) output ZIP; see
  [16_encryption_and_secrets.md](16_encryption_and_secrets.md) if BT_Group later needs an
  encrypted egress ZIP the same way `spec_06`'s `post_export_archive.secret` +
  `pgp_encryption` does.

---

## UC003: ASN.1 Decode + DQ + Quarantine

### Objective

Decode binary/telecom ASN.1 CDR files, enforce DQ expectations, and route violations to a
**specifically named** quarantine table, `BT_Group.asn1_telecom.quarantine_asn1_failures`.

### Schema

Reuses the existing `sample_data/asn1_schema/telecom_cdr_v2.{asn,json}` schema
definition (copied into `BT_Group`'s own `_asn1_schemas` volume, so this catalog is
self-contained rather than depending on `poc`'s copy) -- fields `recordId`, `imsi`,
`msisdn`, `regionCode`, `callDurationSeconds`, `cellId`.

* Bronze target: `BT_Group.asn1_telecom.raw_cdr_records`.
* Quarantine: `BT_Group.asn1_telecom.quarantine_asn1_failures` (set via
  `dq_config.quarantine_table`, an existing override mechanism -- see
  [01_control_metadata_schema.md](01_control_metadata_schema.md) §5).

2 CDR fixtures, generated directly on-cluster (same reasoning as the ZIP fixtures --
binary payloads don't survive workspace sync intact):
`bt_cdr_001.ber` (valid), `bt_cdr_002.ber` (negative `callDurationSeconds` -- violates
`dq_call_duration_non_negative`).

### Validation queries

```sql
SELECT * FROM BT_Group.asn1_telecom.raw_cdr_records;
SELECT recordId, __framework_dq_failed_rule_ids, __framework_dq_failure_reasons, __framework_record_id, __framework_pipeline_run_id, __framework_quarantine_validated_at
FROM BT_Group.asn1_telecom.quarantine_asn1_failures;
```

### Design decisions

* **Exactly 2 fixtures, as scoped** -- one valid, one rule-breaking (negative duration).
  The same `_asn1_decode_error`-based quarantine rule (`dq_asn1_decode_ok`) already covers
  a genuinely malformed/truncated file if one is ever added, without needing a 3rd fixture
  to prove it now.

### Real bug found here (and independently confirmed in `poc` too)

Building UC003 was what surfaced a real, pre-existing bug: `raw_cdr_records` came back
completely empty and both fixtures landed in quarantine with `recordId`/`callDurationSeconds`
both `NULL` -- a full decode failure, not the single intentional DQ violation this UC was
designed to prove. Root cause and fix: see
[13_asn1_dq_quarantine.md](13_asn1_dq_quarantine.md)'s "Real bug found via live deployment"
note -- a literal, never-substituted `{{catalog}}` placeholder inside the (v1-only, since
removed) `telecom_cdr_v2.json` schema-config wrapper's `module_files`, present in **every**
copy of this schema config file, including `poc`'s original (confirmed independently broken
there too, unrelated to BT_Group). Verifying the fix live took two full-refresh attempts to
actually confirm, per [05_deployment_guide.md](05_deployment_guide.md)'s `--full-refresh`
note -- a plain re-run kept showing the identical stale error because Auto Loader had
already checkpointed both `.ber` files as processed; only `--full-refresh-all` reset that
checkpoint and reprocessed them under the fixed code. The v2 redesign (`asn1_schema_path` as
a plain, `{{catalog}}`-substituted `source_config` field) eliminates this entire bug class --
see docs/13's note.

---

## UC004: Intra-Pipeline Reconciliation Drift Audit

### Objective

Detect drift between an independently-ingested reference extract and UC001's
Zerobus-CDC-ingested product catalog, **as a transformation flow inside a DLT pipeline**
(not the standalone `reconciliation/` package + `05_reconciliation_engine.py` notebook
used by [07_reconciliation.md](07_reconciliation.md)'s `spec_09`).

### Why a plain anti-join transformation flow, not the `reconciliation/` package

The existing reconciliation package is deliberately a *separate* job task because its
"self-healing" design writes an out-of-band `MERGE` into a different table as a side
effect (`reconciliation/appender.py`), keyed by a `reconciliation_run_log` idempotency
fingerprint -- a real write side-effect to an unrelated table has no business living inside
a `@dlt.table` function body. UC004's actual requirement, read literally, is narrower:
"route identified drift ... records into an append-only Delta audit table." That's exactly
what an ordinary multi-input transformation flow already does -- no side-effect write, no
new idempotency ledger, just a `SELECT ... LEFT ANTI JOIN ...` producing rows that get
appended to `recon_missing_records_audit` by the same `APPEND` CDC strategy every other
Bronze table in this repo uses. Zero new engine code.

### Schema

* Ingestion: `BT_Group.reconciliation.bronze_reconciliation_source` (`product_id`,
  `product_name`, `category`, `unit_price`, `extract_date`) -- an independent periodic
  extract, seeded with 3 `product_id`s that also exist in UC001's product catalog and 2
  that don't.
* Transformation: `BT_Group.reconciliation.recon_missing_records_audit`
  (`cdc_load_strategy: APPEND`) -- `SELECT ... FROM bronze_reconciliation_source LEFT ANTI
  JOIN <UC001's raw_product_catalog_scd1> ON product_id`, tagged with `drift_type` and
  `drift_detected_at`.

### Cross-pipeline dependency

`source_inputs` includes `BT_Group.bronze_zerobus.raw_product_catalog_scd1` --
**UC001's own target table**, read as a plain batch snapshot (`is_streaming: false`, a
stream-static anti-join). This is a real Delta table (not a `@dlt.view`, which per
[15_engine_refactor.md](15_engine_refactor.md) is never durable outside its own pipeline
update) so it's freely queryable from a different pipeline once UC001 has run at least
once -- `resources/bt_group_test_suite_job.yml` enforces this ordering with an explicit
`depends_on`.

### Validation queries

```sql
-- Expect PC101 and PC102 (not in UC001's source), and NOT PC001/PC002/PC003 (which are)
SELECT * FROM BT_Group.reconciliation.recon_missing_records_audit ORDER BY product_id;
```

### Design decisions

* **One direction of drift, by design.** "Missing," "dropped," and "unpropagated" in the
  requirement all describe the same gap: a record present in the reconciliation source but
  absent from the target. A `LEFT ANTI JOIN` is the correct, minimal expression of that --
  a `FULL OUTER JOIN` would also need to run inside a genuinely streaming context to detect
  the reverse direction (present in target, absent in source), and Structured Streaming
  does not support stream-static `FULL OUTER` joins at all (only `INNER`/`LEFT OUTER` with
  the stream on the left) -- another reason the anti-join framing is both correct to the
  literal requirement and the only one Lakeflow can actually execute incrementally.

---

## UC005: Schema Evolution + Error Handling

### Objective

Prove Auto Loader's
[`addNewColumns` schema-evolution mode](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema)
and `_rescued_data` handle dynamic schema drift without silent data loss, plus a native
"operational alerting" signal via a `warn`-action DQ rule.

### Schema

`BT_Group.bronze_schema_evolution.raw_events` (`device_id`, `event_ts`, `reading`,
`unit`), `schema_evolution_mode: "addNewColumns"`.

Two follow-up batches, deliberately exercising **two different** drift scenarios:

* `uc005_schema_evolution_batch2_rescued_type_mismatch.json` -- `reading` arrives as a
  string (`"SENSOR_ERROR"`) on one record instead of the numeric type inferred from batch
  1. This is a **type mismatch on an existing column**, safely captured in `_rescued_data`
  -- the stream keeps running, no restart needed. **This is the batch auto-tested live in
  this session** (see Relevant tests below).
* `uc005_schema_evolution_batch3_new_column.json` -- introduces `firmware_version`, a
  column that never appeared before. Under `addNewColumns` mode this is **expected** to
  raise `UnknownFieldException` and halt the stream until the pipeline is restarted to pick
  up the new inferred schema -- this is Auto Loader's documented, correct behavior for a
  genuinely new column (the alternative, `schema_evolution_mode: "rescue"`, already used by
  `spec_13`/[14_core_functionality_verification.md](14_core_functionality_verification.md),
  tolerates new columns inline instead). **Deliberately not auto-run** in
  `bt_group_test_suite_job` for the same reason `spec_12`'s negative-validation spec isn't
  wired into a job task -- a task that's *supposed* to fail would pollute the job's
  pass/fail signal. Demonstrate manually:

  ```bash
  databricks fs cp sample_data/bt_group/uc005_schema_evolution_batch3_new_column.json \
    "dbfs:/Volumes/BT_Group/landing/uc005_schema_evolution_zone/events/batch3.json" --profile dev
  databricks bundle run uc005_schema_evolution_pipeline --profile dev  # expected to fail with UnknownFieldException
  databricks bundle run uc005_schema_evolution_pipeline --profile dev  # re-run: picks up firmware_version as a real column
  ```

### Validation queries

```sql
-- After batch2: zero data loss -- the type-mismatched row still has all its OTHER fields intact
SELECT device_id, event_ts, reading, unit, _rescued_data FROM BT_Group.bronze_schema_evolution.raw_events ORDER BY event_ts;
SELECT COUNT(*) FROM BT_Group.bronze_schema_evolution.raw_events WHERE _rescued_data IS NOT NULL;  -- expect exactly 1

-- Confirm the target's actual column list in the metastore
DESCRIBE TABLE BT_Group.bronze_schema_evolution.raw_events;

-- After the manual batch3 + restart demo above: firmware_version becomes a real column,
-- NULL for every row landed before the restart
SELECT device_id, firmware_version FROM BT_Group.bronze_schema_evolution.raw_events ORDER BY event_ts;
```

### Design decisions

* **`dq_no_rescued_data` (`_rescued_data IS NULL`, `action: "warn"`)** -- a native Lakeflow
  expectation (not this framework's `quarantine` extension) that surfaces a per-update
  metric of how many rows got rescued, without failing the pipeline. This is the
  "operational alerting" mechanism the requirement asks for: an operator watching this
  pipeline's expectation metrics sees exactly when rescue is happening, in real time,
  through Lakeflow's own event log -- no custom alerting code needed.

---

## Relevant tests

`tests/integration/test_bt_group_uc001_zerobus_multitable_cdc.py`,
`test_bt_group_uc002_archive_autoloader_egress.py`,
`test_bt_group_uc003_asn1_dq_quarantine.py`,
`test_bt_group_uc004_reconciliation_drift_audit.py`,
`test_bt_group_uc005_schema_evolution.py` -- table existence and row/schema assertions per
use case above, run against a live `bt_group_test_suite_job` pass.

## Example usage

```bash
databricks bundle deploy --profile dev
databricks bundle run bt_group_test_suite_job --profile dev
uv run pytest tests/integration/test_bt_group_uc00*.py --profile dev
```
