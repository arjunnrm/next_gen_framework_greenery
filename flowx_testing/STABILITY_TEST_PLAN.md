# FlowX Stability & Consistency Test Plan — New Workspace

> **What makes this plan different from `TESTING_PLAN.md`.** That plan asks *"does feature X
> work?"* and answers it with one run. This plan asks *"does feature X produce the **same**
> answer every time, and does it stay correct as data arrives, re-arrives, and changes?"* Every
> test case here is run **4–5 times** under a fixed run protocol (§4), and the assertion is as
> much about the **invariants across runs** as about any single run's output.
>
> Target: a **brand-new workspace**. Object footprint is deliberately collapsed to **4 schemas
> and 4 volumes** (§2) — the existing corpus sprawls to 70 schemas / 82 volumes, which is what
> exhausted UC quota on the previous workspaces. Fixtures are **reused**, not regenerated (§3).
>
> Created 2026-08-30.

---

## 1. Non-negotiable settings for every resource in this plan

### 1.1 Retries are off — this is what makes the results mean anything

| Layer | Setting | Default if unset | Why 0 |
|---|---|---|---|
| Pipeline | `configuration: {"pipelines.maxFlowRetryAttempts": "0"}` | **5** for triggered pipelines | With the default, a flow that fails transiently is retried up to 5 times and the update still reports **SUCCESS**. That is precisely the instability this plan exists to measure, silently erased. |
| Job task | `max_retries: 0` on every task | 0 (but set it explicitly) | Same reason one level up: a seed/onboard task that succeeds only on attempt 2 is a finding, not a pass. |
| Pipeline | `development: true` | — | Already set on every test pipeline. Also suppresses automatic update restart. |
| Pipeline | `continuous: false` | — | Triggered updates only, everywhere — including reconciliation, which is triggered-only by design (D5). |

Retries are not attempted for ad-hoc editor updates or Validate updates in any case, so this
setting only bites on the job-driven updates this plan actually runs — which is all of them.

> A flow-retry knob set to 0 means **the first failure of any flow fails the whole update**.
> Expect this plan to surface failures the current suite has been hiding. That is the point.
> Record every one; do not raise the retry count to make a wave go green.

### 1.2 Determinism controls

* One pipeline per test case; **never** share a pipeline between two cases (concurrent updates
  on a shared pipeline produce `Pipeline update already in progress`, which is a harness
  artefact, not a finding).
* Concurrency **≤ 3** across the whole wave.
* No `bundle deploy` while any update is running.
* Every run records: run id, update id, wall-clock, terminal state, and the assertion outputs.

---

## 2. Object layout — 4 schemas, 4 volumes, folder per test case

The existing corpus creates a schema pair and a volume pair *per use case*. This plan does not.
Everything lands in a fixed, flat layout and isolates test cases by **folder**, not by object.

| Schema | Holds | Object count |
|---|---|---|
| `flowx.config` | Control tables + audit + run logs (framework-owned, mandatory) | fixed |
| `flowx.land` | **Volumes only, no tables** | 4 volumes |
| `flowx.bronze` | Every Bronze target + its `_quarantine` companion | 1 table per TC |
| `flowx.silver` | Every transformation / CDC / recon / sink target | 1 table per TC |

### Volumes (4 total, subdivided by folder)

```
/Volumes/flowx/land/raw/<TC_ID>/incoming/      source files land here
/Volumes/flowx/land/work/<TC_ID>/schema/       Auto Loader schemaLocation
/Volumes/flowx/land/work/<TC_ID>/extracted/    ZIP extraction target
/Volumes/flowx/land/work/<TC_ID>/archive/      landing_retention_policy archive
/Volumes/flowx/land/out/<TC_ID>/staging/       sink_config.path
/Volumes/flowx/land/out/<TC_ID>/output/        post_export_archive output
/Volumes/flowx/land/ref/asn1/                  .asn module files (SHARED, read-only)
/Volumes/flowx/land/ref/schema_config/<TC_ID>/ schema_config JSON/YAML
/Volumes/flowx/land/ref/keys/                  PGP test keypairs (SHARED, read-only)
```

**Naming:** every table is `<layer>.<tc_id>_<name>` — e.g. `bronze.zip_001_customer_raw`,
`silver.snap_002_inventory_current`. A test case is therefore fully identifiable from any object
name, and dropping a test case is one `DROP TABLE` + one `rm -r` of its folder.

**Secrets:** one UC secret schema, `flowx.security`, holding the PGP keypair and the AES ZIP
passphrase. Not a fifth data schema — it holds no tables.

> **Why folders and not schemas.** Auto Loader isolation is achieved by `schemaLocation` +
> source path, both of which are folder-scoped. A schema per test case buys nothing the folder
> does not, and costs a UC object against a metastore quota.

---

## 3. Fixture reuse — no new data generated except where a scenario requires it

| Existing fixture | Reused for |
|---|---|
| `sample_data/zip_ingestion/zip_sales_branch_{east,west}.zip`, `zip_ref_{customers,products}.zip` | Suite A (plain ZIP), Suite C (export round-trip source) |
| `sample_data/zip_ingestion/zip_{empty,malformed}.zip` | A5 — corrupt/empty archive handling |
| `sample_data/zip_ingestion/zip_sales_branch_east_duplicate.zip` | A6 — duplicate-by-content detection |
| `sample_data/asn1_cdr_gsm/gsm_cdr_00{1,2,3}.ber` + `asn1_schema/gsm_cdr.asn` | Suite E (ASN.1), all 6 scenarios |
| `asn1_schema/telecom_cdr.asn` / `telecom_cdr_v2.asn` | E5 — schema-version change |
| `finance_usecase/pgp_test_keypair_{private,public}.asc` | Suite B (PGP source decrypt) |
| `finance_egress_usecase/pgp_egress_keypair_{private,public}.asc` | Suite C (PGP export) |
| `inventory_usecase/inventory_snapshot_day{1,2}.csv`, `sample_data/sample_mainframe_customer_master_day{1,2}.csv` | Suite G (`FULL_SNAPSHOT_CDC`) |
| `crm_usecase/customer_scd1_day{1,2}.csv`, `hr_usecase/dim_employee_day{1,2}.csv` | CDC suites |
| `excalibur_usecase/{autoload_batch1,zerobus_source_bus_batch1}.csv` | Suite D (recon) |
| `master_usecase/companies_batch1.csv` | Standardization / normalization suites |

**New fixtures required (3 only):**

1. `wide_usecase/wide_50col_batch{1,2}.csv` — 50 columns, deliberately dirty headers
   (`Customer Id`, ` Region `, `E-Mail Address`, `Country-Code`, …). Suite F.
2. `zip_ingestion/zip_sales_aes.zip` — the *existing* east-branch CSV re-zipped with an AES-256
   passphrase. Suite B1/B3. Generated once by a script, not hand-made.
3. `zip_ingestion/zip_sales_aes.zip.pgp` — (2) PGP-wrapped with the existing test public key.
   Suite B3 (double-layer). Same script.

Everything else in this plan reuses bytes already in the repo.

---

## 4. The run protocol — how "consistency" is actually measured

**Every test case executes the same 4-run sequence.** Snapshot/CDC cases add a 5th.

| Run | Action taken *before* the run | What it proves | Failing here means |
|---|---|---|---|
| **N1** | Land fixture set **A** into an empty `<TC_ID>/incoming/` | Cold start: first-ever checkpoint, first-ever table creation | The feature is broken outright |
| **N2** | **Nothing.** Re-run immediately, identical inputs | **Idempotency.** A no-new-data update must be a no-op | The pipeline is not re-runnable — the single most common production incident |
| **N3** | Land fixture set **B** (different filename, new rows) | **Incrementality.** Only the delta is processed; A's rows are untouched | Reprocessing or data loss on normal arrival |
| **N4** | Re-land set **A** under its **original filename** (overwrite in place) | **Re-delivery.** Vendor re-sends a corrected file under the same name | Silent data loss (ignored) or silent duplication (reprocessed) |
| **N5** *(CDC/snapshot only)* | `--full-refresh` on the pipeline | **Recompute determinism.** Does a rebuild reproduce the same target state? | The table's contents depend on arrival history, not on the data |

### Invariants asserted on every run of every case

| Id | Invariant | How it is checked |
|---|---|---|
| **I1** | `__framework_hash_key` / `__framework_hash_value` for an unchanged row is **byte-identical** across N1→N5 | Store N1's key set; `EXCEPT` against each later run |
| **I2** | No duplicate business key in the target (or a documented reason there is one) | `GROUP BY <pk> HAVING count(*) > 1` |
| **I3** | Quarantine row count exactly matches the rows violating the DQ rule — no more, no fewer | Count against the rule expression run directly on the source |
| **I4** | `config.reconciliation_run_log` shows `SKIPPED_ALREADY_PROCESSED` on N2, never a second append | Query the run log by `reconciliation_id` |
| **I5** | Terminal update state is **identical** across N1–N4 for the same inputs | Compare `pipeline_updates` state; with retries at 0 any variance is real |
| **I6** | Row count is **monotonic non-decreasing** on APPEND targets and **exact** on snapshot targets | `count(*)` per run, recorded in the results table |
| **I7** | `__framework_source_file_name` accounts for every file actually landed — no phantom, no missing | Set-compare against the volume listing |

**A case "passes" only when all 4 (or 5) runs complete *and* every invariant holds.** A case that
passes N1 and fails N2 is a **worse** result than one that fails N1, and must be reported as such.

---

## 5. Functionality → scenario matrix

Priority: **P0** = run first, blocks everything downstream · **P1** = core feature coverage ·
**P2** = run if compute budget allows.

### 5.1 The six deep-dive suites the user named

---

#### Suite A — Normal ZIP source (plain archive) · P0

| TC | Scenario | Fixture | Expected / what is being learned |
|---|---|---|---|
| **A1** | Glob filter: `customer_*.zip` matches one archive, `vendor_feed_*.zip` sits beside it untouched | `zip_ref_customers.zip` + `zip_sales_branch_west.zip` | Only matched archive extracted; non-matching file still present and un-extracted after all 4 runs |
| **A2** | Three archives in one landing folder, one update | east + west + ref_products | All three extracted in a single update; row count = sum |
| **A3** | Re-run with no new archive (this is **N2** for A2) | — | **Extraction marker** (`.<zip>.extracted` sidecar) prevents re-extraction. Assert the marker exists and the extract folder's file mtimes are unchanged |
| **A4** | `delete_source_after_extract` across all three spellings: `false`, `true`, `{action: delete_after_x_days, days: 1}` | east branch, 3 separate TCs | `false` → archive stays forever; `true` → gone after N1; `delete_after_x_days` → **this run's own archive survives**, older ones swept |
| **A5** | A corrupt and an empty archive land alongside a good one | `zip_malformed.zip`, `zip_empty.zip`, east | **Open question:** does one bad archive fail the whole update (retry=0 means no masking), or is it skipped? Record which. |
| **A6** | Same content, two filenames | `zip_sales_branch_east.zip` + `..._duplicate.zip` | `validate_zip_batch` SHA-256 duplicate detection — but note that path runs in `zip_ingestion_pipeline.py`, **not** in `_apply_source_zip_handling`. Confirm whether landing-zone ingestion dedupes at all. |

**Marker-pattern trap to test explicitly (A3b):** `_extraction_marker_path` refuses to write a
marker when `zip_file_pattern` would itself match the marker filename (e.g. pattern `sales_*`
matches `.sales_east.zip.extracted`). Run one TC with such a pattern and confirm the WARNING
fires **and** that the archive is consequently re-extracted on every run — a real, documented
performance cliff.

---

#### Suite B — ZIP with encryption as source · P0 · **largest coverage gap in the corpus**

`grep -l secret_passphrase flowx_testing/*.json` → **no matches**. The AES-256 archive
password path has never been run.

| TC | Scenario | Config | Expected |
|---|---|---|---|
| **B1** | AES-256 password-protected ZIP, no PGP | `pre_extraction_decryption: {secret_passphrase: {...}}` | Extracts via `pyzipper`; first live coverage of this path |
| **B2** | PGP-wrapped ZIP, no archive password | `pre_extraction_decryption: {type: "pgp", private_key_secret: {...}}` | Regression of the existing TC-ING-005 path under the new run protocol |
| **B3** | **Both layers**: PGP envelope around an AES-password ZIP | `type: "pgp"` + `private_key_secret` + `secret_passphrase` together | The validator documents these as independent and combinable. Never tested. Decrypt outer → extract inner. |
| **B4** | PGP private key that is itself passphrase-protected | adds `passphrase_secret` | Optional field, zero coverage. Requires generating one passphrase-protected keypair. |
| **B5** | Wrong passphrase / wrong private key (**negative**) | bad secret value | Must fail with a clear, attributable error naming the archive — not a generic `zipfile.BadZipFile`. Invert the pass assertion. |
| **B6** | Re-run idempotency for an encrypted archive (**N2**) | B3's config | The marker must prevent **re-decryption**, not just re-extraction. The module docstring calls out "re-PGP-decrypted and re-extracted forever" as the cost being avoided — prove it is avoided. Measure N1 vs N2 wall-clock as the evidence. |

---

#### Suite C — ZIP export with PGP (`pgp_zip` Lakeflow sink) · P0

| TC | Scenario | Expected |
|---|---|---|
| **C1** | `format: "pgp_zip"`, encryption on, one micro-batch | One archive in `out/<TC>/output/`, named per `export_file_name_format` |
| **C2** | Multiple micro-batches across N1→N3 | One archive **per batch**; no archive contains another batch's rows. This directly tests the `WriterCommitMessage` contract — `commit()` archives only paths it was handed, never a directory glob |
| **C3** | `pgp_encryption.enabled: false` | Plain ZIP, readable without a key |
| **C4** | Re-run with no new source rows (**N2**) | Does the append_flow re-export already-exported rows? Assert the output folder gains **no** new archive |
| **C5** | **Round-trip proof** | Pull the archive down, PGP-decrypt with `pgp_egress_keypair_private.asc`, unzip, and assert **row-for-row equality** with `silver.<tc>_settlements`. This is the only assertion that actually proves the export is correct rather than merely present |
| **C6** | Missing / malformed recipient public key secret (**negative**) | Fails at graph-definition time (secrets resolve before `write()`), not mid-batch. Confirm the failure point. |

---

#### Suite D — Streaming recon, batch counterpart, appending into a streaming source table · P0

The wiring the user described: `source_config.read_mode: "streaming"`, one `target_configs[]`
entry with `read_mode: "batch"`, and `append_target_table` pointing at a Delta table that is
**itself the streaming source of another pipeline**.

| TC | Scenario | Expected / risk under test |
|---|---|---|
| **D1** | Streaming source + batch target, `comparison_direction: source_to_target`, append into `land`-fed `bronze.<tc>_bus` | Baseline. Note: at most **one** side may be streaming — a stream-stream join is a documented hard stop |
| **D2** | **Append-only invariant.** Downstream pipeline reads `bronze.<tc>_bus` as a streaming source while recon appends corrections into it | The historical SCN-002 defect was a seeder MERGE with `whenMatchedUpdate()` poisoning an append-only stream with `DELTA_SOURCE_TABLE_IGNORE_CHANGES`. Assert the **recon appender only INSERTs** and the downstream stream survives all 4 runs without a full refresh |
| **D3** | Re-run, unchanged source (**N2**) | `SKIPPED_ALREADY_PROCESSED` via the fingerprint; **zero** duplicate correction rows. This is invariant I4 |
| **D4** | Four incremental batches, self-healing convergence | `missing_in_target_count` must strictly decrease and reach 0. Plot it per run — a non-monotonic series is the finding |
| **D5** | **Every run terminates.** A `read_mode: "streaming"` side over the 4-run protocol | Reconciliation is triggered-only: a streaming side runs under `trigger(availableNow=True)`, so each run drains its backlog and **stops**. Assert the task reaches a terminal state unaided, that its checkpoint advances, and that run *k+1* starts where run *k* stopped — no re-reading, no standing query |
| **D6** | `two_tier_verification` true vs false on identical data | Phase-1 fingerprint early-out must produce an **identical** `reconciliation_run_log` classification to the full Phase-2 join. Any divergence is a real correctness bug in the XOR fold |
| **D7** | `comparison_direction` all three values on the same pair | `target_to_source` must **never** append or mutate — audit-only by design. Assert the target table is byte-identical before/after |

---

#### Suite E — ASN.1 binary decode · P1

| TC | Scenario | Expected |
|---|---|---|
| **E1** | BER decode of 3 GSM CDRs with `file_pattern: "*.ber"` | Regression for the fixed `cloudFiles.fileNamePattern` → `pathGlobFilter` defect. This is the only spec in the corpus that sets `file_pattern`, so it is the only guard on that regression |
| **E2** | `asn1_codec: "der"` against the same PDU | Codec selection is honoured; decode succeeds or fails cleanly |
| **E3** | A deliberately truncated `.ber` alongside valid ones | `_asn1_decode_error` populated → DQ rule `_asn1_decode_error IS NULL` routes it to `_quarantine`, valid rows still land. **Note:** this spec's DQ rules reference `_asn1_decode_error` (single underscore) — confirm against the `__framework_` rename whether that column name is still current |
| **E4** | **4-run decode determinism** | Re-land the same 3 files; assert every decoded field value **and** every `__framework_hash_value` is identical. `mapInPandas` compiles the schema once per partition — a partition-count change must not change output |
| **E5** | `telecom_cdr.asn` vs `telecom_cdr_v2.asn` on the same source | Field list is derived from the `.asn` by introspection. A v2 module with an added field: does it widen the target schema or fail? |
| **E6** | ASN.1 files delivered **inside a ZIP** | `_validate_source_zip_handling` is shared by `autoloader` **and** `asn1`, so this is legal and has zero coverage. Combines Suite A and Suite E |

---

#### Suite F — 50 columns, read only 4 (`schema_config`) · P0 · **expect a negative result**

> **Prediction, stated before the run.** `apply_schema_config` does **not** project. Its own
> code path builds `projected` from the declared `columns[]` and then appends
> `passthrough_columns = [F.col(c) for c in df.columns if c not in configured_source_names]`
> (`ingestion/schema_config.py:164-165`), documented as *"Columns not named in `columns[]` pass
> through completely unchanged."* Declaring 4 of 50 columns will therefore land **all 50**.
> There is no `select_columns` / `include_columns` / projection field anywhere in
> `source_config` — confirmed by grep across `spec_validator.py`.
>
> F1 exists to **confirm or refute that prediction on live data**. If confirmed, the finding is
> that the framework has no ingestion-time column-projection capability, and F2 documents the
> only supported workaround.

| TC | Scenario | Expected |
|---|---|---|
| **F1** | 50-column CSV, `schema_config` declaring 4 (cast + rename + comment) | **Predicted: 50 columns in Bronze**, 4 of them cast/renamed/commented. Verify column count and that the 4 comments actually reached UC (`DESCRIBE TABLE EXTENDED`) |
| **F2** | Same source, plus a transformation flow selecting the 4 target columns | The supported way to reach a 4-column table. Measures the cost of F1's gap: an extra flow and an extra table |
| **F3** | `reader_options: {"cloudFiles.schemaHints": "<4 cols>"}` | Hints **type**, they do not project. Confirm 50 columns still land — rules out the obvious workaround |
| **F4** | 50 dirty headers + `column_normalization` + `schema_config` together | **Settles the documented ordering contradiction**: `column_normalization.py`'s docstring says normalization runs *before* `apply_schema_config`; `03_lakeflow_declarative_pipeline.py:242-243` runs `apply_schema_config` **first**. Whichever loses gets corrected |
| **F5** | `schema_config_path` pointing at a **directory** | Latest-mtime resolution (ties → lexicographically largest filename). Drop `schema_v2.json` beside `schema_v1.json` between N2 and N3 and confirm the switch happens **without re-onboarding** |
| **F6** | Column 51 appears in batch B, across all 5 `schema_evolution_mode` values | `addNewColumns`, `addNewColumnsWithTypeWidening`, `rescue`, `failOnNewColumns`, `none` — five TCs sharing one fixture pair |
| **F7** | `schema_config` naming a column absent from the source (**negative**) | `FrameworkConfigError` listing the missing name **and** the available columns |

---

### 5.2 Suite G — `FULL_SNAPSHOT_CDC`, the user's worked example · P0

Run **5 times**, not 4. This suite is the template for what "deep dive on one functionality"
means in this plan.

Snapshot CDC has exactly one shape: a real `target_config.primary_keys` handed to Databricks'
own `dlt.apply_changes_from_snapshot`. A source with genuinely no key is not a snapshot case at
all — it is `TRUNCATE_AND_LOAD`, and G2 covers that fork explicitly.

| Run | Action | Prediction (from `cdc/snapshot.py` + the documented carried limitation) |
|---|---|---|
| **N1** | Land `inventory_snapshot_day1.csv` | Target = day-1 rows, one row per `item_id`. Assert the key is unique (I2) |
| **N2** | Re-run, no new file | **No-op.** Identical row count, identical key set (I1) |
| **N3** | Land `inventory_snapshot_day2.csv` (a key removed, a key changed, a key added) | **Predicted: the removed key is NOT deleted.** `apply_changes_from_snapshot` reads the source dataset's *current contents*; over a streaming Auto Loader upstream those **accumulate**, so the dataset holds day1 ∪ day2. Assert accumulation explicitly rather than asserting the diff that the docs say cannot work |
| **N4** | Re-run, no new file | Still a no-op; the day-1 ∪ day-2 state is stable |
| **N5** | `--full-refresh` | **The interesting one.** Does the target converge, or reproduce the accumulated state? Records whether the table's contents depend on arrival history |

| TC | Variant | Purpose |
|---|---|---|
| **G1** | `FULL_SNAPSHOT_CDC`, `primary_keys: ["item_id"]`, over the 5-run protocol above | The baseline behaviour study |
| **G2** | The same source with **no usable key**, run as `TRUNCATE_AND_LOAD` instead, same 5 runs | The supported route for a keyless full dump. Isolates exactly what a declared key buys over a full recompute — and what it costs |
| **G3** | `FULL_SNAPSHOT_CDC` with `cdc_operation_column` + `delete_values` | Explicit delete markers are filtered **before** the snapshot comparison (`cdc/snapshot.py` drops them from the snapshot-input dataset). Do explicit deletes work where implicit ones cannot? |
| **G4** | `sample_mainframe_customer_master_day{1,2}.csv` with `primary_keys: ["customer_name"]` (TC-CDC-007's shape) | **Update-in-place proof.** Day-2 changes `customer_status` on one customer and `customer_city` on another. Each must stay **one** row that simply takes its new values — never a delete plus a re-insert under a new identity — while the dropped customer is deleted and the new one inserted. 10 rows on both days |
| **G5** | A **composite** `primary_keys` (two columns) where day-2 changes only a non-key column | Same assertion as G4 one level harder: key ordering is spec order, and a multi-column key must still resolve to a single updated row, not a second one |
| **G6** | `FULL_SNAPSHOT_CDC` fed by a **batch** (`materialized_view`) target instead of `streaming_table` | `is_streaming = (target_type == "streaming_table")`. A non-streaming upstream may not accumulate — this is the plausible route to the day-1/day-2 diffing the docs say is unsupported. **Highest-value experiment in this suite.** |
| **G7** | `primary_keys` naming a column the clean upstream does not carry — renamed by `column_normalization`, or dropped by a `data_standardization_sql` projection (**negative**) | `CdcStrategyError` from the snapshot-input dataset's own key guard, naming both the missing key and the available columns. Invert the assertion |

---

### 5.3 Remaining functionality — 4–5 scenarios each

Same run protocol applies. Condensed to scenario names; each expands to a TC row at build time.

| Area | Scenarios (4–5 each) | Pri |
|---|---|---|
| **CDC strategies** | `APPEND` re-delivery · `TRUNCATE_AND_LOAD` full recompute (note E09 is **withdrawn** — `empty_target_if_source_empty` is accepted but inert; assert it has no effect) · `SCD1` day1→day2 in-place update · `SCD2` history rows + current flag across 4 runs · `SCD3` (transformation flows only — rejected on ingestion flows) | P0 |
| **Data quality** | `warn` (rows land, expectation recorded) · `drop` (rows silently removed) · `fail` (**expected failure**, invert assertion) · `quarantine` (companion table, `__framework_dq_*` columns) · a rule referencing a **normalized** column name | P0 |
| **Column handling** | `column_normalization` `lower` / `preserve` / `upper` · collision detection on the lowercased projection (**negative**) · an object omitting `enabled` must resolve to **off** (`enabled` defaults to false — `column_normalization` is the only switch) · a spec still carrying the removed `normalize_column_names` boolean (**negative** — onboarding must reject it with the migration message, not ignore it) · `data_standardization_sql` written against raw names (**negative**, fails at runtime not onboarding) | P0 |
| **JSON handling** | `json_string_columns` with `schema_ddl` · without `schema_ddl` (requires a streaming source) · `explode_columns` on a real array · present-but-empty `explode_columns` = auto-flatten-all · absent = schema-preserving passthrough | P1 |
| **Dedup** | `remove_dups` off · on without watermark · on with `dedup_watermark` · duplicate arriving in a **later** batch (across-run dedup, the hard case) | P1 |
| **Security** | AES `GCM` / `CBC` / `ECB` column encryption · deterministic hashing (same input → same digest across all 4 runs, invariant I1) · redaction · secret resolution failure (**negative**) | P1 |
| **Governance** | Tag application · idempotent re-tagging across 4 runs (no duplicate tags) · tag on a table that does not yet exist · tag value templating | P2 |
| **Transformation** | 4-way join · union all · parameterized filter · transformation over a quarantined upstream | P1 |
| **Reconciliation (batch)** | Drift detection · precomputed hash · `append_target_table` self-healing convergence · fingerprint skip on unchanged source · `on_failure: warn` vs `fail` | P1 |
| **Sinks** | `delta` sink · `kafka` sink · `external_sink` vs pure `sink` target type (pure `sink` bypasses CDC dispatch entirely) · `write_mode` overwrite vs append | P1 |
| **Observability** | Volume JSONL.gz export · OTLP dispatch · structured logging on/off · event-log extraction after a **failed** update (does it still produce telemetry when retry=0 lets the failure through?) | P1 |
| **Storage** | `partition_columns` on `APPEND` (**takes effect**) · same on `SCD2` (**silently inert** — assert it is ignored, not an error) · liquid clustering ≤3 columns · `storage_format: iceberg` | P2 |
| **Parameters** | `{{catalog}}` / `{{env}}` substitution · `${param}` path templating · DQ threshold as a parameter · sink filename as a parameter · a parameter referenced but never supplied (**negative**) | P1 |
| **Onboarding** | `CREATE` · `UPDATE` on an already-onboarded flow · `VALIDATE_ONLY` · bulk directory onboarding · a deliberately invalid spec (**negative**) | P0 |

---

## 6. Execution waves

| Wave | Contents | Gate before proceeding |
|---|---|---|
| **W0** | Provision: 4 schemas, 4 volumes, secret scope, control tables. Upload shared refs (`.asn`, PGP keys). Deploy bundle **once**. | `bundle validate` OK; control tables present; wheel resolvable from the Volume |
| **W1** | Onboarding suite + Suite F (schema/projection) | F1's prediction confirmed or refuted — this decides whether the rest of the plan needs a projection workaround |
| **W2** | Suite A (plain ZIP) → Suite B (encrypted ZIP) | B3's double-layer path works, or is a documented gap |
| **W3** | Suite G (snapshot CDC, 5 runs × 7 variants) + remaining CDC strategies | G6 answers whether day-1/day-2 diffing is reachable at all |
| **W4** | Suite E (ASN.1) + DQ + column handling | — |
| **W5** | Suite D (streaming recon) — **serialised**, never concurrent, since D2 has a downstream stream reading a table another test writes | D2's append-only invariant holds across all 4 runs |
| **W6** | Suite C (PGP export) + sinks + observability | C5 round-trip equality |
| **W7** | Storage, governance, parameters (P2) | Budget-permitting |

Each wave: concurrency ≤ 3, no deploys mid-wave, results written to §8 before the next wave starts.

---

## 7. Predictions on record

Stating these **before** execution so the run confirms or refutes rather than rationalises:

1. **F1** — `schema_config` with 4 of 50 columns will land all 50. No ingestion-time projection exists.
2. **G1/N3** — `FULL_SNAPSHOT_CDC` over a streaming Auto Loader upstream will accumulate day1 ∪ day2 and will not delete the removed key. Declaring a key does not fix the accumulation; it only decides what "the same row" means once the snapshot is read.
3. **G6** — a non-streaming (`materialized_view`) target may not accumulate, and is the plausible route to real snapshot diffing.
4. **N4 (all suites)** — a same-filename overwrite will be **ignored** by Auto Loader; corrected data never lands unless `cloudFiles.allowOverwrites` is set.
5. **A5** — with `pipelines.maxFlowRetryAttempts: 0`, a corrupt archive will fail the whole update rather than being skipped.
6. **F4** — the engine notebook's order (schema_config first) is authoritative and the `column_normalization.py` docstring is wrong.
7. **Retry=0 will surface new failures** in cases that currently report SUCCESS. Any such case is a genuine stability finding, not a regression introduced by this plan.

---

## 8. Results

_(one row per TC per run: run id · update id · terminal state · row count · I1–I7 outcomes · notes)_

| TC | Run | Run id | State | Rows | Invariants | Notes |
|---|---|---|---|---|---|---|
| | | | | | | |

---

## 9. Deliverables

* **Layout:** 4 schemas, 4 volumes, one provisioning notebook (replaces 36 per-usecase seeders)
* **Fixtures:** 3 new (`wide_50col_batch{1,2}.csv`, `zip_sales_aes.zip`, `zip_sales_aes.zip.pgp`) + a generator script; everything else reused
* **Specs:** one JSON per TC under `flowx_testing/stability/`
* **Resources:** one pipeline + one job per TC, every one carrying
  `pipelines.maxFlowRetryAttempts: "0"` and `max_retries: 0`
* **Harness:** a run-protocol driver that executes N1–N5 per TC, lands the right fixture set
  before each run, and records the invariant checks into the §8 table
* **Docs:** this file's §8 populated · `TESTING_STATUS.md` updated · **`RELEASE_NOTES.md` entry**
