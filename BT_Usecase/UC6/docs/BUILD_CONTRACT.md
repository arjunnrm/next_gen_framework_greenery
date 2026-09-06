# BUILD_CONTRACT.md — UC6 Flood Warning System (EA → Leidos)

**Phase A output. Binding on every subsequent step of this build. Read before writing anything.**

If you believe something here is wrong, **flag it back — do not silently deviate.**

Companion documents in this folder:
- [`UC6_Usecase_Explanation.md`](UC6_Usecase_Explanation.md) — business narrative (supplied with the bundle).
- [`UC6_Implementation_Design_flowx.md`](UC6_Implementation_Design_flowx.md) — the supplied technical design.
- [`FRAMEWORK_CAPABILITY_MAP.md`](FRAMEWORK_CAPABILITY_MAP.md) — config-vs-code per capability, and every
  deviation between the supplied brief and the framework's actual behaviour.

---

## 0. The rules that override everything else

1. **Config, not code.** Every UC6 data capability is expressed as an onboarding spec. The only
   hand-written artefacts in this build are (a) the four *framework* enhancements in §9, which are
   generic and reusable, and (b) the test-data generator. No UC6-specific pipeline code exists.
2. **No spec is "done" until validation returns valid.** Unknown attributes are a hard error since
   v1.7.1 — a plausible-looking key is worth nothing.
3. **Onboarding is delegated, never inlined.** Call the generic `onboarding_job` via `run_job_task`.
   Never a `notebook_task` pointing at `02_onboarding_engine.py`.
4. **Isolation from the concurrent UC3 build.** All work happens in the `feature/uc6-flood-warning`
   worktree at `C:/Databricks/uc6-build`. Framework-level commits are kept separate from UC6
   business commits so they rebase cleanly against whatever UC3 lands.

---

## 1. Names — catalog, schema, volume

| Thing | Value | Notes |
|---|---|---|
| Catalog | **`flowx`** (`${var.catalog}`) | Workspace prerequisite; cannot be declared in YAML. |
| Bronze schema | **`flowx.bronze`** | EXISTING, reused. Collision check in §1.1. |
| Silver schema | **`flowx.silver`** | Created if absent by `01_setup_control_tables`-adjacent DDL. |
| Gold schema | **`flowx.gold`** | As above. |
| Landing volume | **`flowx.staging.uc_6`** | New volume under the EXISTING `flowx.staging` schema, mirroring UC3's `uc_3`. |
| Secret (GPG passphrase) | **`flowx.config.pgpkey`** | UC three-level secret. Manual operator prerequisite. |

**Volume layout** (the brief's `raw`/`archive`/`output`, kept verbatim — the framework imposes no
competing convention for a use-case landing volume):

```
/Volumes/flowx/staging/uc_6/raw/        <- all 6 source files land here per cycle
/Volumes/flowx/staging/uc_6/archive/    <- originals moved here after successful ingest
/Volumes/flowx/staging/uc_6/output/     <- the 4 generated output files
/Volumes/flowx/staging/uc_6/_schemas/   <- Auto Loader schema locations (framework requirement)
/Volumes/flowx/staging/uc_6/_extracted/ <- decrypted/decompressed staging for the EA file
```

### 1.1 Collision check — MUST be re-run before onboarding

`flowx.bronze` currently holds UC7 CDR tables (`emsc_cdr_raw`, `psgw_cdr_raw`, `sgsn_cdr_raw`,
`tap310_raw` + quarantines) and, pending the concurrent UC3 build, `physical_device` / `customer` /
`subscriber`. Every UC6 table is prefixed `uc6_`, so no name collides. **Re-verify before onboarding**
— the UC3 session is landing tables into these same schemas concurrently.

## 2. Object names

| Purpose | Name |
|---|---|
| Job | `007_lfj_uc6_ea_flood_warning` |
| Pipeline | `008_ldp_uc6_ea_flood_warning` |
| Dataflow group | `dfg_uc6_ea_flood_warning` |
| Spec file | `onboarding/uc6/uc6_ea_flood_warning.json` |

> **Deviation from the brief, deliberate.** The brief asks for `007_uc6_lfj_EA` / `008_uc6_ldp_EA`.
> Every existing asset in this repo puts the number first and the type second, lowercase:
> `001_lfj_uc7_cdr_asn`, `004_ldp_uc3_excalibur_streaming_cdc`. The repo convention wins per the
> brief's own §0 instruction. **007 and 008 are confirmed free** (001 = UC7, 003–006 = UC3).

`dataflow_id` pattern: `df_uc6_<source>_ingest`. `flow_step_id` pattern: `ts_uc6_<entity>`.

## 3. Sources — the six files, as they ACTUALLY are

Verified by decompressing and field-counting every file in the supplied bundle. **Three findings
contradict the brief and are binding here.**

| Logical file | `file_pattern` (glob) | Delim | Fields | Header | Encryption |
|---|---|---|---|---|---|
| EA flood-risk request | `EE_*-REQUEST_*[Oo][Ff]*.csv.gz.gpg` | `\|` | 14 | **yes** | gzip + **GPG symmetric** |
| CSS Account Address | `CSS_account_address_*.dat.gz` | `\|` | 19 | no | gzip |
| CSS Subscription | `CSS_subscription_*.dat.gz` | `\|` | 47 | no | gzip |
| CSS Account | `CSS_account_*.dat.gz` | `\|` | 56 | no | gzip |
| JT Customer Details | `CM_JT_Customer_Details_*.dat.gz` | `\|` | 34 | no | gzip |
| Excalibur Address | `CM_EXCALIBUR_ADDRESS_*.dat.gz` | **`,`** | 43 | no | gzip |

> **DEVIATION 1 — Excalibur is comma-delimited, not pipe.** The brief states "Pipe (`\|`) is the field
> delimiter inside every `.dat`/`.csv` file". Excalibur has 43 comma-separated fields and exactly 1
> pipe-separated field (i.e. no pipes at all). Its `reader_options.delimiter` is `","`; the other five
> are `"\|"`. Configuring it as pipe would yield a single-column table and silently break the whole
> Excalibur path.
>
> **DEVIATION 2 — the EA file has a header row; the other five do not.** `reader_options.header` is
> `"true"` for EA and `"false"` for the rest. The five headerless sources get their column names from
> `schema_config_path` files mapping positional `_c0.._cN`.
>
> **DEVIATION 3 — `CSS_account_address` glob must not swallow `CSS_account`.** `CSS_account_*.dat.gz`
> matches `CSS_account_address_20250127_00000008.dat.gz` too. The account pattern is therefore
> `CSS_account_[0-9]*.dat.gz`, which the address filename cannot match. **This is a real trap** — the
> two files have different schemas (56 vs 19 fields), so the collision would corrupt both tables.

**Pipe is NOT the record delimiter.** `\n` is; the framework/Spark default is correct, no config needed.

## 4. Framework capabilities — what is config vs. what needed building

Full analysis in [`FRAMEWORK_CAPABILITY_MAP.md`](FRAMEWORK_CAPABILITY_MAP.md). Summary:

| Brief's §7 proposed enhancement | Verdict |
|---|---|
| Pipe delimiter on read | **Already exists** — `reader_options.delimiter`, a shipped reference example |
| Pattern/glob filename matching | **Already exists** — `source_config.file_pattern` → `pathGlobFilter` |
| Archive-move utility | **Already exists** — `landing_retention_policy.clean_source: "archive"` |
| Table tagging helper | **Already exists** — `governance_tags` + `governance/tags.py` |

**Three of the four proposed enhancements already exist.** The genuine gaps the brief did not
anticipate are in §9.

## 5. Table naming & tagging

Pattern `flowx.<layer>.uc6_<entity>` — layer is the *schema*, so the layer name is not repeated in the
table name (the brief proposed `uc6_bronze_ea_request` in a schema that already says bronze).

| Layer | Tables |
|---|---|
| `flowx.bronze` | `uc6_ea_request`, `uc6_css_account`, `uc6_css_account_address`, `uc6_css_subscription`, `uc6_jt_customer`, `uc6_excalibur_address` |
| `flowx.silver` | `uc6_ea_base`, `uc6_ea_address`, `uc6_ee_address_paf`, `uc6_matched_address` |
| `flowx.gold` | `uc6_telephone_output`, `uc6_osapr_output` |

**Tags.** No tag taxonomy or allowlist exists in this repo — `table_tags` and `column_tags[].tags` are
free-form `string→string` maps, validated only for being strings. The brief's proposed tag set is
therefore adopted as-is:

```json
"governance_tags": {
  "table_tags": {
    "use_case": "uc6",
    "source_system": "env_agency|css|jt|excalibur|derived",
    "data_classification": "confidential",
    "pii": "true|false",
    "owner": "business_data_and_ai",
    "environment": "poc"
  }
}
```

`column_tags` is a **list** of `{column, tags}` objects, never a dict keyed by column name.
Tag DDL runs **after** the pipeline update, in the job's `apply_governance` task — never inside the
pipeline graph.

## 6. Match strength — a documented, configurable assumption

The legacy Ab Initio scoring algorithm is **not** in any supplied document. Rather than block the
build or invent a hidden rule, match strength is an explicit, parameterised SQL expression:

- Normalise both addresses (uppercase, strip punctuation, collapse whitespace, drop empty tokens).
- Score = `100 * (matching tokens) / (tokens in the EA address)`, with building number required to
  match when both sides have one.
- Threshold is `${match_strength_threshold}` (default `50`) from `pipeline_parameters`, never a literal.

**This is an assumption, flagged as such in the handback.** It is confined to one `transformation_sql`
expression so it can be swapped wholesale when the real algorithm surfaces.

`${param}` placeholders must be written **unquoted** in SQL — the substituter supplies the quotes.
`WHERE x = '${p}'` renders as `''US''` and is a parse error.

## 7. Onboarding invocation — delegate, never inline

```yaml
- task_key: onboard_uc6
  depends_on:
    - task_key: setup_control_tables
  run_job_task:
    job_id: ${resources.jobs.onboarding_job.id}
    job_parameters:
      spec_file_path: "${workspace.file_path}/onboarding/uc6/uc6_ea_flood_warning.json"
      catalog: ${var.catalog}
      env: ${bundle.target}
      action_type: CREATE
```

Job task chain (mirrors `005_lfj_uc3_excalibur_batch_recon`):

```
setup_control_tables -> onboard_uc6 -> run_pipeline_update -> apply_governance_uc6 -> observability_export
```

The job is **orchestration only**, per the brief. All file handling, decryption, archiving and
transformation happen inside the pipeline. This is not a design choice — the framework enforces it:
ingestion, DQ, crypto and sinks are all pipeline-graph concerns. The one apparent exception,
`apply_governance_uc6`, is a *post-materialization DDL* task, not file I/O; UC tag DDL cannot run
inside a pipeline graph.

## 8. Execution model — batch semantics on a streaming substrate

**There is no batch file reader in this framework.** `source_plane.py` rejects a batch bind of an
ingestion source outright: every `source_type` is unconditionally a `spark.readStream`. UC6's weekly
batch therefore runs as a **triggered** (`continuous: false`) pipeline update over Auto Loader — the
same model UC7 and UC3 already use. One update consumes whatever is in `raw/`, then stops.

Consequence for the sink — and the design decision this contract originally got wrong. The first
draft here said "gold tables are `streaming_table`, and the export sinks read them as streams". That
is impossible for UC6: both gold outputs are heavy `GROUP BY` aggregations, so they are genuinely
`materialized_view` / `TRUNCATE_AND_LOAD`, and Delta refuses to stream from a fully-recomputed table
(`DELTA_SOURCE_TABLE_IGNORE_CHANGES`; the framework's G-STREAM guard rejects it at plan time). Yet
`require_streaming_source()` means a Lakeflow sink accepts only a streaming query. Before v1.7.5 those
two facts made an aggregating result **unexportable** — a structural contradiction, not a config error,
and the first live pipeline run failed on exactly it.

The shipped design: the whole transformation chain is **batch** (bronze `streaming_table` ingestion →
silver and gold `materialized_view`, every `source_inputs[].is_streaming: false`), and the four sinks
set `sink_config.export_trigger: "per_update"` (v1.7.5). That separates the *trigger* (one
`rate-micro-batch` pulse per pipeline, carrying no data, which makes each append flow genuinely
streaming) from the *payload* (the gold MV read as a batch `dlt.read`). Exactly one archive per pipeline
update, including an update that ingested nothing. See `06_egress_and_lakeflow_sinks.md` § "Export
trigger" for the mechanism and the probe evidence behind the pulse source choice.

Two further runtime-only corrections landed the same way (both pass every offline gate and fail only
when the graph is planned or executed): the `ee_address_paf` `UNION ALL` cannot mix one streaming branch
with two batch ones, and `ea_base`/`ea_address` cannot run an unwatermarked `ROW_NUMBER()` over a stream
— both flows are now batch MVs, which is what a full-snapshot union and a per-osapr dedup actually are.

## 9. Framework enhancements — the REAL gap list

These are generic, reusable, and land as **separate commits** from UC6 business config.

| # | Gap | Why UC6 needs it | Shape |
|---|---|---|---|
| **F1** | **Symmetric (passphrase) PGP** | `crypto/pgp.py` is asymmetric-only (armored public/private keys). UC6's EA file and 2 of 4 outputs are GPG **symmetric**, AES256, passphrase from `flowx.config.pgpkey`. | `pgp_decrypt_symmetric(data, passphrase)` / `pgp_encrypt_symmetric(data, passphrase)` via PGPy's `PGPMessage.decrypt(passphrase)` / `PGPMessage.encrypt(passphrase)`. New `pre_extraction_decryption.type: "pgp_symmetric"` handler + `pgp_encryption.passphrase_secret` on the sink. |
| **F2** | **gzip member handling on ingest** | `source_zip_handling` extracts **ZIP** archives via pyzipper. UC6's inbound files are bare `.gz` (and `.csv.gz.gpg`). Spark decompresses a plain `.gz` transparently, but the **GPG-wrapped** one must be decrypted to a staging file first, and the existing path then tries to unzip it. | Extend the pre-extraction path to accept a `gzip` member format so `decrypt → gunzip → land` works without a ZIP container. |
| **F3** | **gzip + delimiter on egress** | The `pgp_zip` sink writes a comma-only CSV (`csv.DictWriter` with no dialect args) inside a ZIP. UC6 must emit `.csv.gz` and `.csv.gz.gpg`, pipe-delimited. | New `sink_config.staged_file_options` (delimiter/header/quoting) and an archive format that emits gzip rather than ZIP. |
| **F4** | **Exporting an aggregating target through a sink** | Discovered on the first live run, not in the brief. A sink is streaming-only and Delta cannot stream a fully-recomputed MV, so UC6's two `GROUP BY` gold outputs had **no export path at all**. | `sink_config.export_trigger: "per_update"` (v1.7.5): one shared `rate-micro-batch` pulse per pipeline drives every such sink; the payload is a batch `dlt.read`. Plus two fixes found the same way: `apply_all_governance_tags` skips `sink` flows (a sink has no table to tag — one sink's `TABLE_OR_VIEW_NOT_FOUND` had failed the whole group's tagging), and every sink path now strips `__framework_*` columns (three lineage columns were leaking into every export file against a 2-column contract). |
| **F4** | **Emptiness / file-presence gate** | `dq_config` rules are **per-row** predicates. Zero rows means zero evaluations, so "fail if a file is missing or empty" can never fire on an ingestion flow. | **Resolved without a framework change** — see below. |

### F4 is config, not code

A **reconciliation flow's** `dq_config` attaches expectations to the one-row `__metrics` dataset,
where `source_record_count` is a real column. So:

```json
{"rule_id": "uc6_ea_request_not_empty", "expression": "source_record_count > 0", "action": "fail"}
```

is a genuine, working emptiness assertion that fails the pipeline update and is recorded in the
reconciliation control tables. UC6 uses one such recon flow per required source. **No enhancement
needed** — the brief's §5.1 "file count > 0 AND row count > 0" gate is met by existing configuration.

*File* count specifically is not separately assertable, but it is not independently meaningful: a
missing file and an empty file both produce zero rows, and both must fail. The row-count gate covers
the requirement.

## 10. Egress — meeting the supplier interface contract

The four outputs are specified by the Leidos/EA interface and are not negotiable:

| Output | Filename | Encoding |
|---|---|---|
| Leidos Telephone | `EE_YYYY-MM-DD-LEIDOS_TELEPHONE_NOfN.csv.gz` | gzip |
| Leidos OSAPR | `EE_YYYY-MM-DD-LEIDOS_OSAPR_NOfN.csv.gz` | gzip |
| Telephone Output | `EE_YYYY-MM-DD-TELEPHONE_NOfN.csv.gz.gpg` | gzip + GPG symmetric |
| OSAPR Output | `EE_YYYY-MM-DD-OSAPR_NOfN.csv.gz.gpg` | gzip + GPG symmetric |

Delivered via F1 + F3. **Decision recorded:** the alternative (ship `.zip` via the existing sink, zero
framework change) was considered and rejected, because it changes what Leidos and EA receive — and
"Leidos/EA see no functional change" is the entire premise of the use case.

## 11. Observability

**`observability.appl_logs` does not exist.** It appears only in the supplied UC6 design document, in
no framework code. Do not create it — the framework has three real surfaces:

1. **Structured JSON logs** — one JSON line per business event to driver stdout, via
   `observability/structured_logger.py::log_flow_event`. Automatic for every flow.
2. **Lakeflow event log** — native per-dataset flow progress and DQ expectation pass/fail counts.
3. **Volume export** — the `observability_export` job task writes `.jsonl.gz` telemetry to a
   `DATABRICKS_VOLUME` destination, configured per dataflow group in the `observability_config`
   control table via the spec's `observability` block.

UC6 routes to `/Volumes/flowx/observability/app_logs/uc6/`, matching UC3's existing use of that
volume. Queries for the handback go in the test report.

## 12. Definition of Done

Per `AGENTS.md`, a change is complete only when every artefact describing it agrees. For UC6 that is:
spec + schema_config files, resource YAMLs, the F1–F3 framework changes with unit tests, validator +
JSON schema + template updates for any new spec key, docs, agent-skill sync, enhancement log, and a
release-notes entry.
