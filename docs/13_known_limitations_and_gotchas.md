# ⚠️ Known Limitations, Silent Traps & Gotchas

> **Read this before filling in an onboarding JSON.** Every entry below is a real behaviour of
> the current codebase, verified against source — not a theoretical risk. They are collected
> here because they share one property: **the onboarding validator cannot catch them.** A spec
> containing any of these is structurally valid, onboards cleanly, and then either fails deep
> inside a pipeline update or — worse — succeeds while producing the wrong data.
>
> Companion to [`12_module_permutation_matrix.md`](12_module_permutation_matrix.md) (what is
> *legal*) and [`00_master_reference_index.md`](00_master_reference_index.md) (what each field
> *means*). This document covers what is legal, meaningful, and still wrong.

---

## Severity legend

| | Class | What it means for you |
|---|---|---|
| 🔴 | **Silent** | Wrong data, no error anywhere. The most dangerous class — you will not find these by reading a run log. |
| 🟠 | **Late failure** | Fails, but at pipeline runtime rather than onboarding. Costs a full build → deploy → run cycle to discover. |
| 🟡 | **Inert** | The field is accepted by the schema and validator but has **no effect**. You will believe a guarantee you do not have. |
| 🔵 | **Operational** | Works correctly, but degrades, costs, or surprises over time. |

---

## Summary — all traps at a glance

### Source & ZIP handling

| | Id | Trap | Field(s) involved |
|---|---|---|---|
| 🔴 | [S1](#s1) | `target_volume_path` and `path` point at different directories → **zero rows ingested, no error** | `source_zip_handling.target_volume_path`, `source_config.path` |
| 🔴 | [S2](#s2) | AES passphrase placed anywhere but inside `pre_extraction_decryption` → silently ignored | `pre_extraction_decryption.secret_passphrase` |
| 🔵 | [S3](#s3) | `zip_file_pattern` matches the framework's own extraction marker → archive re-decrypted and re-extracted **every update** | `source_zip_handling.zip_file_pattern` |
| 🟠 | [S4](#s4) | `delete_source_after_extract` has three spellings with three different lifecycles | `source_zip_handling.delete_source_after_extract` |
| 🔵 | [S5](#s5) | Landing-zone ingestion does **not** deduplicate archives by content | `source_zip_handling` |
| 🟠 | [S6](#s6) | PGP sink/decrypt secrets must resolve before `write()`/`commit()` | `private_key_secret`, `recipient_public_key_secret` |

### Auto Loader & file arrival

| | Id | Trap | Field(s) involved |
|---|---|---|---|
| 🔴 | [A1](#a1) | A file **overwritten under the same name** is never re-ingested | `source_config.path`, `reader_options` |
| 🔴 | [A2](#a2) | `cloudFiles.inferColumnTypes` defaults to **false** → nested JSON arrives as `STRING`, `explode_columns` then fails | `reader_options`, `explode_columns` |
| 🟠 | [A3](#a3) | `cloudFiles.fileNamePattern` does not exist for **any** format | `file_pattern`, `reader_options` |
| 🔵 | [A4](#a4) | `schema_location` shared between two flows corrupts inference for both | `source_config.schema_location` |
| 🟡 | [A5](#a5) | `landing_retention_policy` is rejected on `zerobus`, and needs no `archive_path` | `source_config.landing_retention_policy` |

### Schema, columns & normalization

| | Id | Trap | Field(s) involved |
|---|---|---|---|
| 🔴 | [C1](#c1) | **`schema_config` does not project.** Declaring 4 of 50 columns still lands all 50 | `source_config.schema_config_path` |
| 🔴 | [C2](#c2) | **Every column-referencing field must use the FINAL, post-normalization name** | `data_standardization_sql`, `dq_config.rules[].expression`, `record_id_column`, `primary_keys`, … |
| 🔴 | [C3](#c3) | A `column_normalization` object that omits `enabled` does **not** disable normalization | `source_config.column_normalization` |
| 🔴 | [C4](#c4) | Enabling normalization on an already-deployed flow **renames columns on a live table** | `source_config.column_normalization` |
| 🟠 | [C5](#c5) | Collision detection always runs on the **lowercased** projection, whatever `case` you chose | `column_normalization.case` |
| 🟡 | [C6](#c6) | `nullable` in a schema_config file is documentation only — never enforced | schema_config `columns[].nullable` |
| 🔵 | [C7](#c7) | `schema_config_path` as a **directory** changes behaviour when a file is dropped in — with no re-onboarding | `source_config.schema_config_path` |
| 🟡 | [C8](#c8) | `explode_columns` absent vs present-but-empty are **different**; empty means auto-flatten everything | `source_config.explode_columns` |

### CDC strategies

| | Id | Trap | Field(s) involved |
|---|---|---|---|
| 🔴 | [D1](#d1) | **Full-snapshot CDC over a streaming upstream accumulates.** Removed keys are never deleted | `cdc_load_strategy: FULL_SNAPSHOT_CDC` |
| 🟡 | [D2](#d2) | `empty_target_if_source_empty` (E09) is accepted but **withdrawn and inert** | `target_config.empty_target_if_source_empty` |
| 🟡 | [D3](#d3) | `partition_columns` / `liquid_clustering_columns` are **silently ignored** on every CDC strategy except APPEND and TRUNCATE_AND_LOAD | `target_config.partition_columns` |
| 🟠 | [D4](#d4) | `FULL_SNAPSHOT_CDC_NO_PK` and the whole surrogate-key engine are **removed** — specs carrying either are rejected | `cdc_load_strategy`, `target_config.generate_surrogate_key` |
| 🔴 | [D5](#d5) | v1.3.0 changed the hash construction — **every previously-materialized digest changes value** | `__framework_hash_key`, `__framework_hash_value` |
| 🔵 | [D6](#d6) | `sequence_by_column` falls back to *ingestion* time, not event time | `target_config.sequence_by_column` |
| 🟠 | [D7](#d7) | `SCD3` is rejected on ingestion flows — transformation flows only | `cdc_load_strategy: SCD3` |

### Data quality

| | Id | Trap | Field(s) involved |
|---|---|---|---|
| 🟠 | [Q1](#q1) | `action: "fail"` fails the **entire pipeline update**, by design | `dq_config.rules[].action` |
| 🔵 | [Q2](#q2) | DQ columns were renamed to `__framework_dq_*`; already-deployed tables keep the old names | `__framework_dq_*` |
| 🔴 | [Q3](#q3) | DQ expressions are evaluated against post-normalization names — same trap as [C2](#c2) | `dq_config.rules[].expression` |

### Security & cryptography

| | Id | Trap | Field(s) involved |
|---|---|---|---|
| 🔴 | [E1](#e1) | **Never write a literal `secret(...)` call into a pipeline expression** — Databricks' redaction corrupts the ciphertext | AES column encryption |
| 🟠 | [E2](#e2) | UC secrets are three-level; a classic workspace scope will not resolve | every `*_secret` block |

### Reconciliation

| | Id | Trap | Field(s) involved |
|---|---|---|---|
| 🔴 | [R1](#r1) | Appending into a table that is itself a **streaming source** requires INSERT-only writes | `target_configs[].append_target_table` |
| 🟠 | [R2](#r2) | **At most one side may be streaming.** Stream-stream is unsupported | `read_mode` |
| 🟠 | [R3](#r3) | `type: "table"` means a **Delta** table. Foreign/federated/Parquet/view are rejected | `source_config.type` |
| 🟡 | [R4](#r4) | `target_to_source` never appends and never writes MISSING_IN_SOURCE rows for a `source_to_target` flow | `comparison_direction` |
| 🟠 | [R5](#r5) | Continuous reconciliation is **removed** — it never ran on serverless anyway | `recon_mode` |
| 🔵 | [R6](#r6) | Batch restartability uses a fingerprint; streaming uses the Spark checkpoint — different guarantees | `read_mode` |
| 🟠 | [R7](#r7) | A recon source that is an in-graph `TRUNCATE_AND_LOAD`/SCD/snapshot/MV target **cannot** use `execution_mode: "pipeline"` | `execution_mode`, `cdc_load_strategy` |
| 🔴 | [R8](#r8) | A job-mode recon task left in the YAML beside a pipeline-mode flow runs the comparison **twice** and double-appends | `execution_mode` + `resources/**/*.yml` |
| 🔵 | [R9](#r9) | `publish_schema` defaults to the **hosting pipeline's own** schema — the published audit datasets (`__metrics`/`__mismatch`, v1.6.0-conditional) land beside your business tables | `publish_schema` |

### Sinks & egress

| | Id | Trap | Field(s) involved |
|---|---|---|---|
| 🟠 | [K1](#k1) | Lakeflow sinks accept **streaming queries only** | `target_type: sink` / `external_sink` |
| 🟡 | [K2](#k2) | `target_type: "sink"` bypasses CDC dispatch entirely — `cdc_load_strategy` is never read | `target_type: sink` |
| 🔵 | [K3](#k3) | `sink_config.path` means something different for each format | `sink_config.path` |

### Governance & observability

| | Id | Trap | Field(s) involved |
|---|---|---|---|
| 🟠 | [G1](#g1) | Tag DDL must run **after** the pipeline update, never inside graph definition | `governance_tags` |
| 🟡 | [G2](#g2) | The framework applies tags; it does **not** create the policies that enforce them | `governance_tags` |
| 🟡 | [G3](#g3) | A destination's `mode` decides which engine serves it — get it wrong and it is never exported, or exported twice | `observability_config.mode` |

### Lakeflow platform rules

| | Id | Trap |
|---|---|---|
| 🟠 | [L1](#l1) | A dataset cannot read itself — graph cycle |
| 🟠 | [L2](#l2) | No eager action inside a dataset query definition |
| 🟠 | [L3](#l3) | An `apply_changes_from_snapshot` lambda may not reference **any** pipeline dataset |
| 🟠 | [L4](#l4) | `read_stream` vs `read` must match how the upstream was registered |
| 🔴 | [L5](#l5) | A Delta streaming source must be **append-only** |
| 🟠 | [L6](#l6) | `CREATE OR REPLACE FUNCTION` is idempotent in intent but not atomic |
| 🔴 | [L7](#l7) | A `@dlt.view` is **not** a read-once construct — every consumer opens its own source |
| 🟠 | [L8](#l8) | `pipelines.incompatibleViewCheck.enabled=false` is **known and rejected** — do not set it |
| 🔴 | [L9](#l9) | Reading a pipeline table's **backing storage path** to dodge the self-read ban is forbidden |
| 🔵 | [L10](#l10) | An in-graph healing loop converges in **one update per correction round**, never in-update |
| 🔴 | [L11](#l11) | A **`FULL_SNAPSHOT_CDC`** flow is not in the source plane — reading it must bypass `bind()` |

#### Deployment & operations

| | Id | Trap |
|---|---|---|
| 🔴 | [O1](#o1) | `bundle deploy` during a running update kills it |
| 🔴 | [O2](#o2) | DABs' sync snapshot goes stale — notebooks silently do not update |
| 🟡 | [O3](#o3) | Pipeline flow retries default to 5 — a flaky flow reports SUCCESS |
| 🔵 | [O4](#o4) | UC object quota and serverless concurrency limits produce failures that look like framework bugs |
| 🟠 | [O5](#o5) | `CREATE TABLE IF NOT EXISTS` never adds a column to an **existing** control table — and `bundle deploy` does not run the migration |
| 🟠 | [O6](#o6) | A pipeline whose `artifact_path` belongs to **another** bundle target is orphaned by every deploy of that target |
| 🔴 | [O7](#o7) | The **v1.6.0 upgrade renames/unpublishes intermediates** — streaming state resets; plan the first post-upgrade update per pipeline |
| 🔴 | [O8](#o8) | A module loaded from a **job task** must not transitively `import dlt` — it dies at import time |

---
---

## Source & ZIP handling

### <a id="s1"></a>S1 🔴 `target_volume_path` and `path` must point at the same directory

**This is the single most common ZIP misconfiguration.** They are two separate fields with no
relationship enforced anywhere:

```jsonc
"source_config": {
  "path": "/Volumes/cat/crm/landing_zip/extracted/customer/",       // where Auto Loader READS
  "source_zip_handling": {
    "target_volume_path": "/Volumes/cat/crm/landing_zip/extracted/" // where the ZIP is UNPACKED
  }                                                    // ^^^ MISMATCH — note the missing /customer/
}
```

**What happens if they differ:** the archive is found, decrypted, and extracted successfully.
Auto Loader then reads a directory the members were never written to. The pipeline update
**succeeds**, the target table gains **zero rows**, and nothing anywhere logs a problem. On a
first run you get an empty table; on a subsequent run you get an unchanged one.

**Why it isn't caught:** `_apply_source_zip_handling` reads `target_volume_path` straight from
the spec and hands it to `extract_encrypted_zip`; the reader then independently calls
`.load(source_config["path"])`. `target_volume_path` appears exactly **once** in
`onboarding/spec_validator.py` — a `check_string(..., required=True)`. There is no cross-check.

**Rule:** `target_volume_path` must be **identical to**, or a **parent directory of**,
`source_config.path`. Identical is strongly preferred — a parent works only because Auto Loader
recurses, and it will then also pick up every *other* flow's extracted members that share that
parent.

**Related:** `source_zip_path` is a **directory**, never a single file — a real landing zone
accumulates more than one archive between updates, which is why `zip_file_pattern` is required
whenever `source_zip_handling.enabled` is true.

---

### <a id="s2"></a>S2 🔴 The AES ZIP passphrase lives inside `pre_extraction_decryption`

Two independent, **combinable** encryption layers exist, and they are configured in the same
object:

```jsonc
"source_zip_handling": {
  "enabled": true,
  "source_zip_path": "/Volumes/cat/fin/landing/incoming/",
  "zip_file_pattern": "*.zip.pgp",
  "target_volume_path": "/Volumes/cat/fin/landing/extracted/txns/",
  "pre_extraction_decryption": {
    "type": "pgp",                        // LAYER 1: outer envelope on the whole file
    "private_key_secret":  { "secret_catalog": "...", "secret_schema": "...", "secret_key": "..." },
    "passphrase_secret":   { ... },       //   optional: the PRIVATE KEY's own passphrase
    "secret_passphrase":   { ... }        // LAYER 2: the AES-256 password on the ZIP itself
  }
}
```

Three easily-confused fields:

| Field | Protects | Optional? |
|---|---|---|
| `private_key_secret` | The PGP envelope around the file | Required **when** `type: "pgp"` |
| `passphrase_secret` | The PGP **private key** itself | Optional |
| `secret_passphrase` | The **ZIP archive's** AES-256 password | Optional, independent of `type` |

**What happens if you misplace it:** an unrecognised key inside `pre_extraction_decryption` is
simply not read. `extract_encrypted_zip` is then called with every `secret_*` argument `None`,
which is its "plain, unprotected ZIP" path — extraction fails on the password-protected archive
with a generic `ArchiveError`, giving no hint that your passphrase was never used.

Omit `type` entirely for a password-protected ZIP with no PGP envelope. Omit
`secret_passphrase` for a PGP-wrapped ZIP with no archive password. Set both for a
PGP envelope around a password-protected ZIP — decrypt outer, then extract inner.

---

### <a id="s3"></a>S3 🔵 A too-broad `zip_file_pattern` disables re-extraction protection

The framework drops a hidden sidecar marker, `.<zipname>.extracted`, next to each processed
archive so the next update does not re-decrypt and re-unpack it. If your `zip_file_pattern`
would *also* match that marker filename, the marker cannot safely be written — it would be
treated as an archive on the next pass.

The framework detects this, logs a **WARNING**, and continues **without a marker**. The archive
is then re-decrypted and re-extracted on **every single pipeline update**, forever. On a large
PGP-encrypted archive that is a substantial, permanent, and entirely silent cost.

**Rule:** keep `zip_file_pattern` anchored on a real extension — `"*.zip"`, `"orders_*.zip"`.
Avoid bare prefixes like `"orders_*"`, which match `.orders_x.zip.extracted`.

---

### <a id="s4"></a>S4 🟠 `delete_source_after_extract` has three spellings and three lifecycles

| Spelling | This run's archive | Older archives in the landing zone |
|---|---|---|
| `false` | Kept **forever** | Kept forever |
| `true` | Deleted immediately after successful extraction | Untouched |
| `{"action": "delete_now"}` | Same as `true` | Untouched |
| `{"action": "delete_after_x_days", "days": N}` | **Kept** — deliberately not swept by its own run | Swept when older than `N` days |

`days` is only meaningful with `delete_after_x_days`; the validator rejects it on `delete_now`
rather than ignoring it. Deletion happens **only on a successful extraction** — a failed
extraction leaves the archive in place, deliberately, so the only remaining copy is not
destroyed as the error propagates.

---

### <a id="s5"></a>S5 🔵 Landing-zone ingestion does not deduplicate archives by content

`archive/zip_ingestion_pipeline.py::validate_zip_batch` computes a SHA-256 per archive and
reports duplicates — but that function belongs to the **explicit multi-ZIP batch job**, driven
by a `zip_ingestion_configs/*.json`. The ingestion-flow path
(`ingestion/readers.py::_apply_source_zip_handling`) does **not** call it.

So the same content landed twice under two names in a landing zone is extracted twice and
ingested twice. Use `remove_dups` on the ingestion flow if that matters — but read
[dedup's own caveats](#c8-related) first.

---

### <a id="s6"></a>S6 🟠 PGP secrets resolve at graph-definition time, never inside the sink

For the `pgp_zip` sink, `commit()` and `abort()` do **not** run in the pipeline driver process —
they run in a separate "python streaming data source runtime" worker with no working `dbutils`
gateway. A first implementation resolved keys lazily inside `commit()` and failed every time
with `Unable to resolve Unity Catalog secret ...: [Errno 13] Permission denied`.

All sink secrets are therefore resolved **before** the sink is constructed. Practical
consequence: **a bad secret reference fails at graph definition, not mid-batch** — which is the
good outcome, but means you see the failure before any data moves, not after.

---

## Auto Loader & file arrival

### <a id="a1"></a>A1 🔴 An overwritten file is never re-ingested

Auto Loader's file registry keys on the file **path**, not its content or modification time. A
vendor re-sending `orders_20260830.csv` with corrected values, under the same name, into the
same directory, is **not** reprocessed. The pipeline update succeeds and the corrections never
land.

**Fix:** `"reader_options": {"cloudFiles.allowOverwrites": "true"}`.

**But understand the consequence before setting it:** on an `APPEND` target the file's rows are
appended **again**, producing duplicates rather than corrections. `allowOverwrites` is only
safe in combination with a CDC strategy that can absorb a re-delivery (`SCD1`, `SCD2`,
`FULL_SNAPSHOT_CDC*`) or with `remove_dups`. The genuinely safe pattern is for the producer to
land every delivery under a **new filename**.

---

### <a id="a2"></a>A2 🔴 `cloudFiles.inferColumnTypes` defaults to false — nested JSON arrives as STRING

A JSON source with a nested array reaches the framework as a `STRING` column unless
`inferColumnTypes` is on. Declaring `"explode_columns": ["metrics"]` against it then fails at
runtime:

```
FrameworkConfigError: explode_columns names 'metrics', which is type string
-- only struct or array columns can be exploded/flattened
```

**Fix — the supported one:** declare the shape explicitly, which works on batch and streaming
alike and does not depend on inference:

```jsonc
"json_string_columns": [
  {"column": "metrics", "schema_ddl": "array<struct<sensor:string,val:double>>"}
]
```

`parse_json_string_columns` runs immediately **before** `apply_explode_columns`, so the parsed
struct flows straight into the flatten pass. The no-`schema_ddl` form (a bare column name) falls
back to Databricks' inferring `from_json` and therefore **requires a streaming source with a
checkpoint** — prefer the explicit `schema_ddl` form.

---

### <a id="a3"></a>A3 🟠 `cloudFiles.fileNamePattern` does not exist — for any format

Auto Loader validates `cloudFiles.`-prefixed keys against a closed whitelist **without
consulting `cloudFiles.format`**. Setting this option fails the update immediately:

```
[CF_UNKNOWN_OPTION_KEYS_ERROR] Found unknown option keys: cloudFiles.filenamepattern
```

Use the spec's own `source_config.file_pattern`, which the framework maps to the generic,
un-prefixed `pathGlobFilter` for every format. Do **not** work around this with
`cloudFiles.validateOptions=false` — that suppresses the validator instead of removing the
invalid key.

---

### <a id="a4"></a>A4 🔵 `schema_location` must be unique per flow

`schema_location` holds Auto Loader's inferred schema **and** its file-tracking state. Two
flows sharing one location will interleave their schema versions and their processed-file
registries. Symptoms are confusing: unexpected `_rescued_data` content, columns appearing from
another feed, files that "were already processed" on their first-ever run.

Convention: `/Volumes/{{catalog}}/<domain>/_schemas/<target_table>/` — one directory per
**target table**, never per domain.

---

### <a id="a5"></a>A5 🟡 `landing_retention_policy` is autoloader/asn1 only

It maps to `cloudFiles.cleanSource`, which exists only on an Auto Loader file read. Setting it
on a `zerobus` flow is a hard onboarding-time error — a `zerobus` source streams an existing
Delta table and has no landing zone to clean:

```
<path>.landing_retention_policy: only applicable to source_type 'autoloader'/'asn1'
(Auto Loader file ingestion), but this flow's source_type is 'zerobus'
```

`archive_path` is never required; an empty one degrades the policy to off rather than failing.

---

## Schema, columns & normalization

### <a id="c1"></a>C1 🔴 `schema_config` does not project — you cannot "read only 4 of 50 columns" with it

This is the most commonly assumed capability the framework does **not** have.

`apply_schema_config` casts, renames, and comments the columns you declare — and then appends
**every column you did not declare**, unchanged:

```python
passthrough_columns = [F.col(c) for c in df.columns if c not in configured_source_names]
result_df = df.select(*projected, *passthrough_columns)
```

Its own docstring states it: *"Columns not named in `columns[]` pass through completely
unchanged."* A 50-column source with a 4-column `schema_config` lands a **50-column** Bronze
table, with 4 of them cast/renamed/commented.

**There is no ingestion-time column-projection field anywhere in `source_config`** — no
`select_columns`, no `include_columns`, no `drop_columns`. `cloudFiles.schemaHints` does not
help either: it influences **types**, not which columns are read.

**The supported way to get a narrow table:** ingest wide into Bronze, then declare a
`transformation_flow` whose `transformation_sql` selects the 4 columns into Silver. You pay for
one extra flow and one extra table, and you keep the other 46 columns in Bronze for audit —
which is usually what you want anyway.

---

### <a id="c2"></a>C2 🔴 Every column-referencing field must use the FINAL, post-transformation name

This is the trap behind "I turned on column normalization and my transformation broke."

**The actual runtime order** (`notebooks/03_engine/03_lakeflow_declarative_pipeline.py`):

```
raw source read
  1. apply_schema_config            renames declared in the schema_config file
  2. column_normalization           trim · case-fold · [^A-Za-z0-9_] → _ · collapse _ · strip edge _
  3. attach_technical_metadata      adds __framework_* columns
  4. parse_json_string_columns      STRING → struct
  5. apply_explode_columns          flatten / explode
  6. remove_dups                    full-row dedup
  7. apply_data_standardization_sql ← LAST. sees only the final names.
```

**Every one of these spec fields is written against the names as they exist at step 7:**
`data_standardization_sql`, `dq_config.rules[].expression`, `dq_config.record_id_column`,
`explode_columns`, `json_string_columns[].column`, `target_config.primary_keys`,
`sequence_by_column`, `partition_columns`, and every reconciliation `match_keys` /
`compare_columns`.

#### What actually breaks, precisely

Case alone is **forgiving** — Spark resolves column references case-insensitively by default
(`spark.sql.caseSensitive = false`). So with `case: "lower"` producing `region`, an expression
written `TRIM(UPPER(REGION))` still resolves. With `case: "upper"` producing `REGION`, an
expression written `TRIM(UPPER(region))` also still resolves.

**What is not forgiving is character substitution.** Normalization replaces every character
outside `[A-Za-z0-9_]` with an underscore:

| Source header | After normalization (`lower`) | Reference that works | Reference that **fails** |
|---|---|---|---|
| `Company Name` | `company_name` | `company_name`, `COMPANY_NAME` | `` `Company Name` `` |
| `E-Mail Address` | `e_mail_address` | `e_mail_address` | `` `E-Mail Address` ``, `email` |
| `Region ` (trailing space) | `region` | `region`, `Region` | `` `Region ` `` |
| `Country-Code` | `country_code` | `country_code` | `` `Country-Code` `` |

```jsonc
// CORRECT — post-normalization names
"data_standardization_sql": [
  "TRIM(UPPER(region)) AS region",
  "LOWER(TRIM(e_mail_address)) AS e_mail_address"
]

// WRONG — raw source names. Onboards cleanly. Fails at pipeline runtime.
"data_standardization_sql": [
  "TRIM(UPPER(`Region `)) AS region",
  "LOWER(TRIM(`E-Mail Address`)) AS email"
]
```

**Why onboarding does not catch it:** `spec_validator.py::_validate_data_standardization_sql`
enforces *grammar only* — one column expression per entry, ending in `AS <name>`, no
`SELECT`/`FROM`/`JOIN`/`;`. Column **existence** is a runtime fact resolved lazily by Spark. The
spec onboards clean and the update later dies with `UNRESOLVED_COLUMN`, naming the bad
reference.

**Rule:** decide `column_normalization` **first**, write down what your column names become, and
then write every other field against that list.

---

### <a id="c3"></a>C3 🟠 `normalize_column_names` is removed — a spec still carrying it is rejected

There is now exactly **one** way to turn normalization on:

| Spec | Result |
|---|---|
| `"column_normalization": {"enabled": true}` | **On**, `case` = `lower` |
| `"column_normalization": {"enabled": true, "case": "upper"}` | **On**, `case` = `upper` |
| `"column_normalization": {"case": "preserve"}` — no `enabled` | **Off**. Absent means off. |
| `"column_normalization": {"enabled": false}` | **Off** |
| *(no `column_normalization` at all)* | **Off** |
| `"normalize_column_names": <anything>` | **Onboarding error** — the key is removed in v1.4.0 |

Two traps, both from the pre-v1.4.0 behaviour:

1. **An object with only `case` used to be ON.** It deferred enablement to the legacy boolean.
   With that boolean gone there is nothing to defer to, so `{"case": "preserve"}` on its own now
   reads as written: off. If a spec relied on that deferral, add the explicit `"enabled": true` —
   otherwise the flow silently stops renaming.
2. **The legacy boolean is rejected, not ignored.** That is on purpose: ignoring
   `normalize_column_names: true` would flip renaming from on to off with no signal, and the first
   symptom would be raw names with spaces reaching Delta — failing the write, or rewriting an
   existing table's column names.

Migration is a one-line edit; see [`02_ingestion_and_sources.md`](02_ingestion_and_sources.md) for
the full table.

---

### <a id="c4"></a>C4 🔴 Turning normalization on for a deployed flow renames columns on a live table

Normalization is opt-in per flow, and deliberately so: enabling it changes the DataFrame's
column names, which changes the schema written to an **existing** Delta target. Depending on the
target's schema-evolution settings this either fails the write or silently rewrites the table's
column names, orphaning every downstream query, view, and dashboard that referenced the old
ones.

**Rule:** treat enabling (or changing the `case` of) `column_normalization` on a flow with a
materialized target as a **breaking schema change**. Plan it as a migration — new target table,
or a full refresh with downstream consumers notified — never as a config tweak.

---

### <a id="c5"></a>C5 🟠 Collision detection always uses the lowercased projection

Whatever `case` you choose, the duplicate check runs over the **lowercased** projection of the
produced names. Unity Catalog and Spark resolve column references case-insensitively, so two
output names differing only by case would be an unusable table, not a valid one.

```
Region  → Region      ┐ case: "preserve"
region  → region      ┘ → both fold to "region" → FrameworkConfigError
```

This means `case: "preserve"` and `case: "upper"` cannot be used to keep two columns that differ
only by case. That is intentional: the framework proves the deterministic lowercase projection is
unique before letting any casing reach the target.

The character normalization itself (`[^A-Za-z0-9_]` → `_`, collapse runs, strip edges, fall back
to `"_"`) is **not** configurable. Only the case-fold step is.

---

### <a id="c6"></a>C6 🟡 `nullable` in a schema_config file is documentation only

It is accepted, preserved as metadata, and **never enforced** — Spark's `.cast()` cannot force a
column non-nullable independently of the data flowing through it.

To genuinely enforce non-nullability, add a companion DQ rule:

```jsonc
"dq_config": {"rules": [
  {"rule_id": "dq_customer_id_not_null", "expression": "customer_id IS NOT NULL", "action": "fail"}
]}
```

---

### <a id="c7"></a>C7 🔵 A directory `schema_config_path` changes behaviour without re-onboarding

`schema_config_path` accepts an exact file **or** a directory. Given a directory, the file with
the most recent modification time wins (ties broken by the lexicographically-largest filename).

That is a genuine convenience — drop `schema_v2.json` into a Volume directory and the next
pipeline update picks it up with no spec change and no re-onboarding. It is also a genuine
hazard: **anyone with write access to that directory can change your ingestion schema**, and
nothing in the control tables or the audit log records that it happened.

Use an exact file path for anything you need to be able to reconstruct after the fact.

---

### <a id="c8"></a>C8 🟡 `explode_columns`: absent, `null`, and `[]` mean three different things

| Spec | Behaviour |
|---|---|
| key omitted entirely | **Schema-preserving pass-through** — nothing flattened |
| `"explode_columns": null` | Same as omitted (this is how a scaffolded template says "not configured") |
| `"explode_columns": []` | **Auto-flatten everything** — recursively flattens every struct and explodes every array anywhere in the schema |
| `"explode_columns": ["a","b"]` | Flatten/explode exactly those top-level columns |

The present-but-empty case is the surprise. It exists so a spec can opt into a full "flatten this
whole JSON document" pass, but a hand-edited spec that empties the array meaning "turn this off"
gets the **opposite** of what it intended — historically this caused silent cartesian row
explosion. Remove the key to turn it off.

Each named column must resolve at runtime to a `StructType` or `ArrayType`; the validator only
checks it is a list of strings, since it has no access to the source's runtime schema.

<a id="c8-related"></a>**Related — `remove_dups` caveats:**
* **Unbounded streaming state.** `dropDuplicates` on a stream retains every distinct row key
  seen since the stream started, forever. On a long-running streaming table this grows without
  limit and will eventually degrade or fail the pipeline. Set `dedup_watermark` for any
  continuously-running stream. A runtime WARNING is emitted (once per flow) when dedup runs on a
  streaming DataFrame with no watermark.
* **Which duplicate survives is arbitrary.** Spark gives no guarantee, so the surviving row's
  `__framework_source_file_name` / `__framework_ingestion_timestamp_utc` are an arbitrary pick.
  If you need "keep the latest", use an SCD strategy or a windowed `transformation_sql`, not
  full-row dedup.
* `__framework_*` and other technical columns are **excluded** from row identity by design —
  including them would make every redelivered row unique and defeat dedup entirely.

---

## CDC strategies

### <a id="d1"></a>D1 🔴 Full-snapshot CDC over a streaming upstream accumulates — deletes never happen

`dlt.apply_changes_from_snapshot` treats its source dataset's **current contents** as the latest
full snapshot. Over a streaming Auto Loader upstream those contents **accumulate**: once a Day-2
extract lands, the dataset holds Day-1 ∪ Day-2.

Consequences, both silent:

* A key present on Day 1 and **absent** on Day 2 is **never deleted** from the target.
* A key **modified** on Day 2 appears as two rows in the snapshot input.

**Single-snapshot flows are correct.** Multi-snapshot Day-1/Day-2 diffing is **not supported**
by this wiring, and cannot be, because no named pipeline dataset can mean "only the most recently
arrived snapshot."

**If you need real snapshot diffing:** the supported Lakeflow pattern is the lambda form reading
a versioned **path** per snapshot — legal precisely because a path is not a pipeline dataset (see
[L3](#l3)). Adopting it requires a new spec contract for locating snapshot versions and changes
how DQ/quarantine applies to snapshot flows. It is deliberately not implemented.

**Practical guidance while filling in the JSON:** use `FULL_SNAPSHOT_CDC*` when each pipeline run
consumes exactly one complete extract and the landing zone is cleared between runs (see
`landing_retention_policy`). If your landing zone accumulates extracts, use `SCD1`/`SCD2` with a
real `sequence_by_column` instead.

---

### <a id="d2"></a>D2 🟡 `empty_target_if_source_empty` is accepted but inert

v1.3.0 added this guard (E09) to stop a zero-record extract silently blanking a populated
`TRUNCATE_AND_LOAD` target. It was **withdrawn on 2026-08-29**: preserving the previous contents
means the target reads **itself**, which Lakeflow rejects outright as a graph cycle
(`Graph is not topologically sorted. There is a cycle between <target> and <target>`), and every
`TRUNCATE_AND_LOAD` pipeline failed 100% of the time.

The field remains **valid in a spec and has no runtime effect.** A `TRUNCATE_AND_LOAD` target is
currently **unguarded** against an empty source: a zero-record extract will blank it. Enforcing
this needs a post-update check outside the pipeline graph.

---

### <a id="d3"></a>D3 🟡 `partition_columns` and `liquid_clustering_columns` only work on APPEND and TRUNCATE_AND_LOAD

| `cdc_load_strategy` | Physical layout applied? |
|---|---|
| `APPEND` | ✅ Yes |
| `TRUNCATE_AND_LOAD` | ✅ Yes |
| `SCD1`, `SCD2`, `SCD3` | ❌ **Silently ignored** |
| `FULL_SNAPSHOT_CDC` | ❌ **Silently ignored** |

Only `APPEND`/`TRUNCATE_AND_LOAD` register a real physical `@dlt.table` directly from
`target_config`. Every CDC-dispatched strategy registers a `@dlt.view`, and the table it feeds is
created later by the CDC dispatcher, which never reads these fields. Not a validation error —
just inert. Liquid clustering is capped at **3 columns**.

---

### <a id="d4"></a>D4 🟠 `FULL_SNAPSHOT_CDC_NO_PK` and the surrogate-key engine are removed (v1.4.0)

Five spec attributes and one CDC strategy no longer exist, and onboarding **rejects** each by name
rather than ignoring it:

| Removed | Replacement |
|---|---|
| `cdc_load_strategy: "FULL_SNAPSHOT_CDC_NO_PK"` | `FULL_SNAPSHOT_CDC` + real `primary_keys`, or `TRUNCATE_AND_LOAD` |
| `target_config.generate_surrogate_key` | — (declare `primary_keys`) |
| `target_config.surrogate_key_columns` | — (`primary_keys` for identity, `columns_to_check` for change detection) |
| `target_config.surrogate_key_exclude_columns` | — (as above, or `columns_to_exclude`) |
| `reconciliation_flows[].generate_surrogate_key` | — (the flow's `match_keys`) |

**Why this is graded 🟠 and not 🔵.** The failure is loud at onboarding, so nothing runs wrong. But
the *migration decision* is a data-modelling one and it is easy to get quietly wrong: choosing
`FULL_SNAPSHOT_CDC` with a key that is not actually unique in the snapshot produces a target that
looks fine and silently collapses rows. Verify the candidate key's uniqueness in the source before
committing to it — `SELECT k, count(*) FROM src GROUP BY k HAVING count(*) > 1` — and use
`TRUNCATE_AND_LOAD` if it fails.

**What changed behaviourally for a migrated flow.** The old strategy hashed the entire payload into
`__framework_surrogate_key`, so a Day-2 field change was a *delete of the old row plus an insert of
a new one* — the same entity under two identities. With a declared key, that same change is
reported as the `UPDATE` it always was. Expect the first post-migration snapshot to look like a
full delete-and-reinsert (the diff key changed), then correct behaviour thereafter.

**Legacy columns are left alone.** A table materialized before the upgrade still physically carries
`__framework_surrogate_key`. It is not dropped, it stays excluded from comparison-column
resolution, and `storage/column_ordering.py` no longer front-loads it — so it drifts to the end of
the schema on the next rewrite. Harmless either way.

---

### <a id="d5"></a>D5 🔴 v1.3.0 changed the hash construction — every existing digest changes value

The canonical construction is now:

```sql
sha2( concat_ws('||', coalesce(trim(lower(cast(c1 as string))), '__NULL__'), ...), 256 )
```

Before v1.3.0 this existed in **three** places (`cdc/hashing.py`, the since-deleted
`crypto/hashing.py`, `reconciliation/appender.py`) that had already drifted apart in their exclusion sets — a drift
that is *silently wrong* rather than loudly broken, because two sides of a reconciliation would
disagree about what "the same row" hashes to and nothing would report an error.

**Consequence of the consolidation: every `__framework_hash_key` and `__framework_hash_value`
this framework has ever materialized changes value the moment the new wheel is deployed.** Existing tables keep their old digests until rewritten; a reconciliation
between an old-digest table and a new-digest one will classify **every row** as drifted. Plan a
full refresh of both sides together, or re-materialize before reconciling.

---

### <a id="d6"></a>D6 🔵 `sequence_by_column` falls back to ingestion time, not event time

`dlt.apply_changes` always needs a monotonic sequencer. When `sequence_by_column` is not set, the
framework uses `__framework_ingestion_timestamp_utc` — **when the framework read the row**, not
when the business event happened.

For a re-landed or back-filled file that is the *re-ingestion* time, so an older business record
ingested later will win against a newer one ingested earlier. Always set an explicit
`sequence_by_column` when the source carries a real event/update timestamp.

---

### <a id="d7"></a>D7 🟠 `SCD3` is transformation-flow only

`ALLOWED_INGESTION_CDC_STRATEGIES` excludes `SCD3`; `ALLOWED_TRANSFORMATION_CDC_STRATEGIES`
includes it. The validator's own reason: *"SCD3 pivots current/previous state via an internal
history table, which only makes sense downstream of a raw ingestion flow."*

SCD3 has no native Lakeflow equivalent — it is built by materializing full history into a hidden
`_<target>_scd2_history` table and pivoting the two most recent versions per key into
`current_<col>`/`previous_<col>`.

**Related:** SCD2 registers a companion `<target_table>_current` reporting dataset aliasing
`__START_AT`/`__END_AT` to `valid_from`/`valid_to`/`is_current`. Despite the name, it is a real
`@dlt.table`, not a view — a `@dlt.view` cannot take a multipart qualified name, and is not a
durable, queryable catalog object once the defining update finishes.

---

## Data quality

### <a id="q1"></a>Q1 🟠 `action: "fail"` fails the entire pipeline update

By design — it maps to Lakeflow's `expect_or_fail`. One violating row raises
`ExpectationViolationException` and the update terminates. Every downstream table in that update
is left unwritten.

This makes any `fail` rule an **all-or-nothing gate on the whole pipeline**, not on the flow.
Prefer `quarantine` unless you genuinely want the update to stop.

**For test harnesses:** a `fail` rule that fires is the *expected* outcome, so job-level terminal
state must be **inverted** for those cases — a FAILED job is a PASSED test.

---

### <a id="q2"></a>Q2 🔵 DQ and technical columns were renamed to `__framework_*`

Every framework-generated column now carries the `__framework_` double-underscore prefix:
`__framework_dq_failed_rule_ids`, `__framework_dq_failure_reasons`,
`__framework_dq_quarantine_flag`, `__framework_quarantine_validated_at`,
`__framework_pipeline_run_id`, `__framework_record_id`, `__framework_source_file_name`,
`__framework_source_file_size`, `__framework_source_file_modification_time`.

`_rescued_data` was deliberately **not** renamed — it is a native Auto Loader column, not
framework-owned.

**There is no automatic migration.** Already-deployed tables keep the old single-underscore names
until manually altered or recreated. A bug report mentioning `_dq_*` or `_record_id` is most
likely an old table, not a code defect.

---

### <a id="q3"></a>Q3 🔴 DQ expressions use post-normalization column names

Identical to [C2](#c2), and worth calling out separately because it is easy to miss: a rule like
`"expression": "\`E-Mail Address\` IS NOT NULL"` written against raw headers will not resolve
once normalization is on. DQ rules run downstream of the whole ingestion pipeline.

---

## Security & cryptography

### <a id="e1"></a>E1 🔴 Never embed a literal `secret(...)` call in a pipeline expression

Building `aes_encrypt(CAST(col AS STRING), secret('scope','key'), 'MODE')` as SQL text inside a
Lakeflow dataset expression **silently corrupts every ciphertext**. Confirmed live: 100% of
encrypted values decoded back to byte-length-inflated strings riddled with `U+FFFD` replacement
characters — the signature of a lossy UTF-8 round trip.

**Root cause:** Databricks' platform-wide credential-redaction machinery
(`spark.redaction.regex`, which matches the keyword `secret`) treats any expression whose *query
text* contains a `secret(...)` call as credential-bearing and mutates it as part of Lakeflow's
per-dataset metrics/observability layer. It is specific to Lakeflow's logging layer — isolated
non-DLT Structured Streaming writes with identical SQL never reproduce it.

**What the framework does instead** (Databricks support's own confirmed workaround): resolve the
secret's plaintext once via `resolve_secret_value` *before* building the expression, then pass it
to PySpark's native `functions.aes_encrypt` as a literal `Column` (`F.lit(...)`) — so the
substring `secret(` never appears in the expression driving the write.

**A check that does not catch this:** asserting a column is "not plaintext". Corrupted ciphertext
is also not plaintext. Only a genuine **round-trip decrypt-and-compare** assertion detects it.

**Accepted trade-off:** the resolved key exists as a Python string on the driver for the duration
of one flow's graph-definition call.

---

### <a id="e2"></a>E2 🟠 Secrets are Unity Catalog three-level references

Every `*_secret` block in a spec is a three-part UC reference, never a classic workspace-level
secret scope:

```jsonc
{"secret_catalog": "flowx", "secret_schema": "security", "secret_key": "pgp_private_key_finance"}
```

Passphrases and keys must **always** be a genuine secret reference — an inline literal is
rejected by `check_secret_ref`.

---

## Reconciliation

### <a id="r1"></a>R1 🔴 Appending into a table that is a streaming source requires INSERT-only writes

The self-healing pattern — reconcile, then append corrections into `append_target_table` — is
safe **only if that table is written append-only**. If the target is also the streaming source of
another pipeline, any operation that rewrites an existing row (a `MERGE` with
`whenMatchedUpdate()`, even when the values are byte-identical) permanently breaks the downstream
stream:

```
DELTA_SOURCE_TABLE_IGNORE_CHANGES / DELTA_MERGE_UNRESOLVED_EXPRESSION
```

This is not hypothetical: an idempotent seeder that MERGEd into `zerobus_source_bus` poisoned its
consumer on the **second** run and required a full refresh to recover
(`databricks pipelines start-update <id> --full-refresh` — note: **not** `--full-refresh-all`,
which is not a flag).

**Rules:**
* Any table feeding a streaming reader must only ever be **appended to**.
* Any seeder or fixture loader for such a table must be **insert-only**, not MERGE-idempotent.
* The reconciliation appender is insert-only by design — but anything *else* writing to the same
  table must be too.

**Related — the matcher's duplicate-key handling exists for this exact pattern.** An
append-only bus legitimately holds a prior correction *alongside* the stale row it corrects.
Classifying per joined row let the same source record count as both matched and unmatched at
once, so the self-healing append never converged and re-appended a fresh duplicate every run.
Rows sharing a `__framework_hash_key` are now collapsed to one representative outcome
(MATCHED > VALUE_DRIFT > MISSING_*) before classification.

---

### <a id="r2"></a>R2 🟠 At most one side of a comparison may be streaming

`source_config` **or** a given `target_configs[]` entry may set `read_mode: "streaming"` — not
both. A stream-stream join would need watermarking and supports only inner/left-outer semantics,
which is incompatible with the framework's four-way
MATCHED / MISSING_IN_TARGET / MISSING_IN_SOURCE / VALUE_DRIFT classification.

Mixing freely in the *supported* direction is fine: a streaming source against a batch target, or
a batch source against a streaming target.

---

### <a id="r3"></a>R3 🟠 `type: "table"` means a Delta table specifically

As of v1.3.0 `type: "table"` is the only accepted dataset type, and the resolved provider is
asserted to be Delta before the read. Unity Catalog also holds foreign/federated tables, Parquet
and CSV external tables, and views — none of which can carry precomputed
`__framework_hash_key`/`__framework_hash_value`, give a stable per-read snapshot, or support file
skipping on `filter_condition`.

The removed `type: "file"` / `type: "sink"` branches read a raw location directly. That had no
transaction boundary — a file landing mid-run silently changed what "the source" meant between
the fingerprint and the append. **The supported answer is: land the files into a Delta table
first, then reconcile against the table.**

A provider that cannot be *determined* (a temporary view, or a catalog that does not answer
`DESCRIBE TABLE EXTENDED`) is logged at WARNING and allowed through — refusing on unreadable
metadata would fail runs that work today.

---

### <a id="r4"></a>R4 🟡 `comparison_direction` gates action, not classification

The matcher always classifies **every** record in one pass regardless of direction — computing
all four outcomes is effectively free once the join has run. What `comparison_direction` gates is
what the framework *does*:

| Direction | Appends the miss set? | Mismatch-logs |
|---|---|---|
| `source_to_target` | ✅ | MISSING_IN_TARGET, VALUE_DRIFT |
| `target_to_source` | ❌ **never** — audit-only by design, never mutates the target | MISSING_IN_SOURCE |
| `both` | ✅ | all three |

A `source_to_target` flow still gets `missing_in_source_count` populated in
`reconciliation_run_log` (part of the same free aggregation), but **no MISSING_IN_SOURCE rows are
written** to `reconciliation_mismatch_log`. That is deliberate: writing an audit trail for a
direction the author did not ask to audit would be misleading, not merely extra.

---

### <a id="r5"></a>R5 🟠 Continuous reconciliation is removed — schedule the job instead

`recon_mode` is gone in v1.4.0, from the spec, the schema, the `reconciliation_flow_spec` DDL, the
validator and the notebook widgets. A spec still carrying it — **either** value, `"triggered"`
included — fails onboarding.

Two reasons it went, and the second is the substantive one:

1. **It never ran where this framework runs.** Continuous mode kept the `foreachBatch` handler
   under a standing trigger, so the job task never returned. On serverless job compute that raises
   `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED` — and every job in this framework runs on serverless.
2. **It wrapped a standing stream around a batch-shaped unit of work.**
   `run_target_reconciliation` writes one `reconciliation_run_log` row per invocation and checks a
   batch fingerprint for idempotency. Under a never-ending query that produced a log row per
   micro-batch whose fingerprint could never repeat, and counts describing an arbitrary slice of
   wall clock rather than a comparison anyone asked for.

**Replacement:** every run is now `trigger(availableNow=True)` for a streaming side and a plain
batch read otherwise — it processes everything currently available and stops, matching a bounded
job task's lifecycle. **To reconcile more often, schedule the job more often.** A job on a
15-minute schedule gives bounded, individually-idempotent, individually-auditable answers, which is
what `reconciliation_run_log` was built to hold.

---

### <a id="r6"></a>R6 🔵 Batch and streaming restartability use different mechanisms

| `read_mode` | Restart guarantee | Consequence |
|---|---|---|
| `batch` | `reconciliation_run_log.source_batch_fingerprint` — a deterministic hash of the miss set. A prior SUCCESS with the same fingerprint makes the run a no-op (`SKIPPED_ALREADY_PROCESSED`) | Re-running against an unchanged source never appends duplicate corrections |
| `streaming` | Spark Structured Streaming's own **checkpoint** (source offsets). The fingerprint column is explicitly "batch read_mode only" | A cancelled run resumes where the checkpoint left off. **Deleting the checkpoint reprocesses everything** |

`task_run_id` also behaves differently: in `triggered` mode it narrows the *static* side's read to
the producing run; the *streaming* side is never narrowed (its micro-batch boundary already is the
batch boundary, and filtering would discard offsets the checkpoint has already advanced past). In
`continuous` mode neither side is narrowed. In both modes it still travels into every log row as a
correlation value.

---

### <a id="r7"></a>R7 🟠 A `TRUNCATE_AND_LOAD` (or SCD / snapshot / MV) recon source cannot use `execution_mode: "pipeline"`

`execution_mode: "pipeline"` registers the prepared source `_recon__<rid>__src` as a **streaming
table**, because the L5 pulse streams it — that is what makes healing fire once per source-advancing
update. **A Delta stream may only read an append-only table.** If the reconciliation source is a
dataset this same pipeline publishes, and its producing flow does not write it append-only, the
streaming read is illegal.

| Producer of the recon source, in this same group | Why not append-only | `"pipeline"` legal? |
|---|---|---|
| `APPEND` | plain append | ✅ |
| `TRUNCATE_AND_LOAD` | a `@dlt.table` fed by a **full recompute** — every update *replaces* its contents | ❌ |
| `SCD1` / `SCD2` / `SCD3` | `dlt.apply_changes` — real `MERGE`/`UPDATE`/`DELETE` writes | ❌ |
| `FULL_SNAPSHOT_CDC` | `dlt.apply_changes_from_snapshot` | ❌ |
| `target_type: "materialized_view"` | fully refreshed on every update | ❌ |

**The fix is one word: `execution_mode: "pipeline_audit_only"`.** L3 and L4 still run in-update, so
the comparison, `__metrics` and the `dq_config` expectation are unaffected; only the corrective
append moves back to the standalone `05_reconciliation_engine.py` job task.

**Why this is graded 🟠 and not 🔵.** `TRUNCATE_AND_LOAD` was **missing** from the plan-time guard
(`engine/source_plane.py::_NON_APPEND_ONLY_CDC_STRATEGIES`, which listed only the four CDC
strategies) — it dispatches to no CDC strategy at all, so it slipped through. Such a flow passed
**every** onboarding and plan-time check and then failed at pipeline **runtime** with:

```
DELTA_SOURCE_TABLE_IGNORE_CHANGES
```

`TRUNCATE_AND_LOAD` is now in the guard set, so the failure is a plan-time `FrameworkConfigError`
naming the consumer, the locator, the producing flow and `'pipeline_audit_only'` as the setting to
use. **Delta's own escape hatch, `skipChangeCommits`, is refused outright** — it silently drops
every changed row instead of failing, which is strictly worse than failing the update. See
[L5](#l5) and [`07_reconciliation_engine.md` §11.7](07_reconciliation_engine.md#117-the-reconciliation-source-must-be-append-only-in-pipeline-mode).

**Real instance:** the geneva tariffs group reconciles against `geneva_admin.stg_tariffelementband`,
which is that same group's own `TRUNCATE_AND_LOAD` ingestion target — which is why
`flowx_testing/053_geneva_e41a47ba_recon_in_pipeline.json` declares `pipeline_audit_only`. That
scenario is verified **offline only** (validator + `plan_source_plane`, pinned by
`tests/unit/test_geneva_e41a47ba_topology.py`); it has never been confirmed by a live run, because
its target table's grants block the pipeline's run-as identity — see [O6](#o6).

---

### <a id="r8"></a>R8 🔴 A leftover job-mode recon task beside a pipeline-mode flow runs the comparison twice

Flipping a reconciliation flow to `execution_mode: "pipeline"` (or `"pipeline_audit_only"`) changes
**metadata only**. It does not touch your DABs resources, and nothing anywhere cross-checks them.

A `run_<n>_reconciliation` notebook task still sitting in `resources/<...>_job.yml` therefore keeps
running the *standalone* `05_reconciliation_engine.py` against the same `reconciliation_id` — so the
comparison happens **twice per cycle**, once inside the pipeline update and once as the job task,
and **each pass appends its own corrections** into `append_target_table`.

**Why 🔴 rather than 🟠:** both passes succeed. Two SUCCESS run-log rows, two sets of appended
corrections into an append-only bus, no error anywhere. The batch fingerprint
([R6](#r6)) does not save you — the two passes are independent invocations against a source the
first pass may already have moved, so the fingerprints legitimately differ.

**This is not hypothetical.** `resources/feature_tests/flowx_test_002_003_job.yml` still carried its
`run_003_reconciliation` task after scenario 003 was flipped to `"pipeline"` — while the file's own
header already claimed the task had been removed — risking a double-append into
`Excalibur_usecase.zerobus_source_bus`. The task was deleted.

**Rule:** the same change that sets `execution_mode` to a pipeline mode must delete that flow's
standalone reconciliation task in the same commit. Grep the `resources/` tree for the
`reconciliation_id` before you deploy.

---

### <a id="r9"></a>R9 🔵 `publish_schema` defaults to the hosting pipeline's own schema

Under `execution_mode: "pipeline"` / `"pipeline_audit_only"` a flow publishes real, externally
visible UC tables — since v1.6.0 that is `recon__<rid>__<tid>__metrics` / `__mismatch` (each
registered only when its `logging_config` capture flag resolves true) plus a healing flow's
`_recon__…__src`/healing `__tgt`; the classification and every other L3/L4 node are now
pipeline-scoped temporary tables that publish nowhere (see
[`07` §11.10](07_reconciliation_engine.md#1110-v160--the-intermediate-object-rule-and-the-conditional-audit-datasets)).
Omitting `publish_schema` puts the published ones in the pipeline's **own** target schema, beside
the business tables it publishes. That is legal, and rarely what anyone wanted.

`notebooks/03_engine/03_lakeflow_declarative_pipeline.py` resolves that default as
`PIPELINE_SCHEMA`, first non-empty wins:

1. `spark.conf` `pipelines.schema` (the pipeline's declared target schema, current key)
2. `spark.conf` `pipelines.target` (its pre-`schema` spelling, still set by older pipelines)
3. `spark.catalog.currentDatabase()`
4. `GROUP_ROW.target_schema` from the dataflow group's control-table row

All four missing logs a WARNING saying any flow without an explicit `publish_schema` will fail when
its datasets are named.

**Steps 1 and 2 exist because of a real failure.** `PIPELINE_SCHEMA` originally had no chain at all
— just `currentDatabase()`. During **graph definition** the session's current database is *not* the
pipeline's target schema, so it resolved to `None` even for a pipeline plainly declaring
`schema: bronze_excalibur`, and the first node-naming call died with:

```
ValueError: Unsafe or malformed target_schema: None
```

Observed live on 2026-08-31 (pipeline `be78d88d`). It affected **only** flows relying on the
default; a flow setting an explicit `publish_schema` never went near that path. Setting
`publish_schema` explicitly, to a dedicated audit schema in the pipeline's own catalog, is the
recommendation for anything beyond a test fixture.

---

## Sinks & egress

### <a id="k1"></a>K1 🟠 Lakeflow sinks accept streaming queries only

Per Lakeflow's own documentation: *"Only streaming queries are supported. Batch queries are not
supported."* `dlt.create_sink` / `@dlt.append_flow` cannot be fed by a batch query at all.

The framework raises `FrameworkConfigError` up front naming the flow, rather than letting Lakeflow
fail deep inside graph resolution. Practically: a `sink` or `external_sink` flow needs
`target_type: "streaming_table"` semantics upstream — a `materialized_view` or `batch_table`
target cannot feed one.

---

### <a id="k2"></a>K2 🟡 `target_type: "sink"` bypasses CDC dispatch entirely

`cdc_load_strategy` is **not read at all** for a pure `sink` target. No main table, no quarantine
table, no CDC — the staged view feeds the sink directly. This is what satisfies "sink nodes must
not appear as persisted datasets."

Quarantined rows are still filtered out before reaching the sink, and the `__framework_dq_*`
process columns are dropped, so internal columns never leak to an external system.

`external_sink` is different: it **does** materialize a real, governed, DQ-quarantined table
first, and then additionally exports it via a second `@dlt.append_flow`.

---

### <a id="k3"></a>K3 🔵 `sink_config.path` means something different per format

| `format` | What `path` is |
|---|---|
| `delta` | The Delta table/directory path — the actual output |
| `pgp_zip` | A **per-micro-batch staging location**. The real output is `post_export_archive.output_zip_path` |
| `kafka` | **No filesystem path at all** — not required |

Pointing a `pgp_zip` sink's consumers at `sink_config.path` gets them the staging area, not the
archives.

---

## Governance & observability

### <a id="g1"></a>G1 🟠 Tag DDL must run after the pipeline update

`ALTER TABLE ... SET TAGS` and `ALTER TABLE ... ALTER COLUMN ... SET TAGS` are Unity Catalog DDL
against a **materialized** table. They must be invoked after a pipeline update has created or
updated the target — never from inside the pipeline's own graph-definition code, where the table
does not yet exist.

`apply_governance_tags` is therefore called from a downstream orchestration step, not from the
graph. Tag DDL is naturally idempotent: re-applying an identical key-value pair is a no-op, and a
changed value for an existing key simply overwrites it. No ledger table is needed.

---

### <a id="g2"></a>G2 🟡 The framework applies tags; it does not enforce them

A column tag like `mask=PII` or a table tag like `row_filter=region_restricted` is a
**declarative label**. What (if anything) enforces masking or filtering based on that label is a
workspace admin's Unity Catalog tag-policy configuration, entirely external to this framework.

Applying a `mask=PII` tag does **not** mask anything by itself.

---

### <a id="g3"></a>G3 🟡 A destination's `mode` decides which engine serves it

| `mode` | Served by | Lifecycle |
|---|---|---|
| `"triggered"` (default) | `08_dlt_observability_engine.py` | Bounded post-update export, a downstream job task |
| `"continuous"` | `06_event_log_otel_streaming_pipeline.py` | Always-on pipeline streaming N event-log tables |

There is deliberately no entrypoint that switches between them — the two have fundamentally
different lifecycles. `mode` exists purely to stop one destination being served, and therefore
**double-exported**, by both. A destination whose `mode` does not match the engine you actually
run is simply never exported.

`mode` is read defensively (`row.get("mode") or "triggered"`), so an older control table without
the column degrades to `triggered` rather than failing.

---

## Lakeflow platform rules

> These are properties of Lakeflow itself, not of this framework. Each cost a full
> build → deploy → run cycle to discover, because **none of them fails locally** — the unit suite
> is happy and only a real pipeline update surfaces them.

### <a id="l1"></a>L1 🟠 A dataset cannot read itself

`Graph is not topologically sorted. There is a cycle between <target> and <target>`, raised
before a single flow runs. Any design where a target's query definition reads its own previous
contents is rejected outright. This is what withdrew [D2](#d2).

**The subtlety that costs a deploy cycle: `spark.read.table("<my own fully-qualified name>")` is
NOT an escape.** Inside a pipeline, a plain Spark read of a name the same pipeline publishes is
**intercepted and resolved as a graph reference**, exactly as if you had written `dlt.read(...)` —
so it forms the same cycle and aborts the same way. Going through `spark` rather than `dlt` changes
the spelling, not the graph.

> **Documentation correction (v1.5.0).** `dq/quarantine.py`'s `_quarantine_table` docstring asserted
> the opposite — that such a read fetches *"the table's PRIOR materialized state from outside the
> pipeline graph."* That is wrong, and the docstring is corrected in v1.5.0. The code path is
> unwired today, but it sits in the module this release touches, and a future author trusting the
> old sentence would build a design that cannot run.

The framework now defends this rule four times, at decreasing cost of discovery: a plan-time Kahn
topological sort (`engine/source_plane.py::assert_acyclic`, raising `FrameworkGraphCycleError` and
naming the exact ring) before a single `dlt` call; a binding rule that never gives an in-graph
locator a source-plane node; the `V-CYC-1..5` onboarding rules in `spec_validator.py`; and an AST
test asserting no closure reads the dataset it defines.

### <a id="l2"></a>L2 🟠 Never put an eager action inside a dataset query definition

The closure runs during graph **construction**, when an upstream produced by the same update
legitimately holds no data yet. `df.isEmpty()` / `limit(1).take(1)` therefore cannot distinguish
*"source is empty"* from *"source not yet materialized"*, and will fire on flows whose source is
never empty.

**Corrected in v1.5.0 — the blanket form of this rule is too broad, and this repo already
contradicts it in production.** `dq/quarantine.py::_quarantine_table` runs
`upstream.agg(F.count(F.lit(1)), F.sum(...)).collect()[0]` inside a live `@dlt.table` closure on the
**batch** branch, with the comment *"This closure body runs at Lakeflow's graph-EXECUTION time"*;
only the **streaming** branch is guarded, and its stated reason is that *"a streaming DataFrame
cannot be eagerly aggregated/collected here"*. The three prohibitions that are actually real:

1. **An eager action on a STREAMING plan** — Spark raises *"Queries with streaming sources must be
   executed with writeStream.start()"*.
2. **A self-read** — see [L1](#l1).
3. **A side-effecting write** (`saveAsTable`, `insertInto`, a control-table upsert) inside a query
   definition. Side effects belong in a `foreach_batch_sink` handler, which is execution-time code
   *outside* any query definition and where `.collect()`, `.count()`, `spark.sql` and `try`/`except`
   are all legal.

The batch-empty-source trap above is still real and still bites; what is *not* true is that every
eager action is banned everywhere. `tests/unit/test_recon_registration_ast.py` enforces exactly the
three prohibitions above and names the `quarantine.py` precedent in its docstring, so no future
change re-broadens the rule and breaks shipped code.

### <a id="l3"></a>L3 🟠 An `apply_changes_from_snapshot` lambda may not reference any pipeline dataset

Three errors in sequence, each only visible after fixing the previous:

1. `dlt.read()` on a pipeline-local view → `TABLE_OR_VIEW_NOT_FOUND` (the lambda runs outside
   graph-element registration, so the name resolves via the metastore)
2. Materializing it → `REFERENCE_DLT_DATASET_OUTSIDE_QUERY_DEFINITION`
3. → `View ... is a streaming view and must be referenced using readStream`

**The working shape:** register a real `@dlt.table` snapshot-input dataset, do the filtering and
guards *inside* it, and pass `apply_changes_from_snapshot` that dataset's **name**. A **path**
(`spark.read.load(...)`) *is* legal inside the lambda — a path is not a pipeline dataset.

### <a id="l4"></a>L4 🟠 `read_stream` vs `read` must match how the upstream was registered

Reading a streaming view with batch `dlt.read()` raises
`View <name> is a streaming view and must be referenced using readStream`. `is_streaming` is
`(target_type == "streaming_table")` — full stop; `source_type` plays no part.

### <a id="l5"></a>L5 🔴 A Delta streaming source must be append-only

See [R1](#r1) for the full account and recovery procedure, and [R7](#r7) for the in-graph form of
the same rule — a reconciliation flow in `execution_mode: "pipeline"` streams its source, so that
source's producing flow must write it append-only.

### <a id="l6"></a>L6 🟠 `CREATE OR REPLACE FUNCTION` is idempotent in intent but not atomic

Concurrent `setup_control_tables` runs — and every `flowx_test_*` job starts with one — race
on UC function creation. The loser gets `[ROUTINE_ALREADY_EXISTS]`, every downstream task is
skipped, and it reads as a framework failure. Handled by
`schema_provisioner.is_already_exists_race()`, which treats "someone else created it" as success
while still failing loudly on permission, missing-schema, quota, and syntax errors.

### <a id="l7"></a>L7 🔴 A `@dlt.view` is not a read-once construct

A `@dlt.view` is **inlined into every consumer's plan**. Declaring a source once as a view and
reading it from three places produces **three independent physical reads** of the underlying table
or path — three `DeltaSource`s, or three Auto Loader streams. Nothing errors; the cost and the side
effects simply happen N times.

Two concrete consequences seen in this repo:

* An Auto Loader ingestion flow with two quarantine rules opened **two independent `cloudFiles`
  streams over one path sharing one `cloudFiles.schemaLocation`** — see [A4](#a4) for why one
  schema location per stream is not optional.
* A path carrying `landing_retention_policy` or `source_zip_handling` runs its **file-lifecycle side
  effects once per consumer**: `cloudFiles.cleanSource` *moves or deletes* committed landing files,
  and ZIP handling PGP-decrypts, unzips and writes `.__framework_extracted__` markers.

**Only materialization makes "read once" literally true.** That is what `engine/source_plane.py`
does: it registers one materialized `_src__…` node per external read identity — a **streaming
table** if any consumer streams, a materialized view otherwise — and every consumer binds to it.

**Since v1.7.3 this happens at every fan-out, including 1** (the Single-Read architectural
mandate): N distinct source tables produce N base ingestion nodes. Previously fan-out 1 stayed
inline, because materializing a single-consumer read costs a full physical copy and destroys
predicate pushdown of that consumer's filter into the original source — which is why
`source_plane.materialize` used to default to `"auto"`. It now defaults to `"always"`; that
pushdown cost is knowingly paid in exchange for a topology that does not change shape with
fan-out. `"never"` was removed and is rejected at onboarding time and again at plan time.

**Materialized ≠ published (v1.6.0).** Since v1.6.0 these shared nodes are
`@dlt.table(temporary=True)` under their bare names — materialized once per update, so everything
above still holds, but **invisible in Unity Catalog** unless the spec sets both
`source_plane.catalog` *and* `source_plane.schema` to publish them deliberately. The same
Intermediate Object Rule makes a multi-reader `_<target>_staged` intermediate a temporary table
rather than a published one, and does the same to the reconciliation L3/L4 plumbing (with the
exceptions listed in [`07_reconciliation_engine.md` §11.10](07_reconciliation_engine.md#1110-v160--the-intermediate-object-rule-and-the-conditional-audit-datasets)).
Never conclude from an empty catalog listing that the node was not materialized — check the
pipeline's own graph/event log. Upgrading an existing deployment: see [O7](#o7).

### <a id="l8"></a>L8 🟠 `pipelines.incompatibleViewCheck.enabled=false` is known and rejected

When a batch consumer reads a streaming view, Lakeflow's error text
(*"View `X` is a streaming view and must be referenced using readStream"*, see [L4](#l4)) names
`pipelines.incompatibleViewCheck.enabled=false` as an escape hatch. **Do not set it, and do not add
it to a spec's `spark_config`.** It is recorded here as evaluated and rejected for two independent
reasons:

1. It is **pipeline-wide**. It would be applied through `engine/spark_config.py` and would disable
   the guard for *every* dataset in the group, not the one you were arguing with.
2. It **silences the check without making a streaming plan batch-readable.** The incompatibility is
   in the plan, not in the warning.

The supported resolution is structural, not a flag: a shared source-plane node is registered as a
**materialized streaming table whenever any consumer streams**, and one such table is legally
readable by `dlt.read_stream` *and* `dlt.read` in the same update. A view can serve neither pair.

### <a id="l9"></a>L9 🔴 Reading a pipeline table's backing storage path is forbidden

Because [L1](#l1) blocks reading your own dataset by name, the tempting next move is to read its
**backing storage location** instead — `spark.read.format("delta").load("<the table's path>")` —
since [L3](#l3) establishes that a *path* is not a pipeline dataset. It works often enough to look
like a technique. **It is forbidden in this framework, and no framework code does it.**

* It reads a **stale, arbitrary** snapshot: which commit you get depends on where the update
  happens to be, and Lakeflow has no ordering edge to the producing flow because you have hidden the
  dependency from the graph.
* It is exactly the cycle [L1](#l1) rejects, with the safety check removed — the same design, minus
  the error message that would have told you.
* It couples the pipeline to a physical location UC is free to change, and it bypasses every UC
  permission and lineage check performed on the table name.

If you need a table's prior state, publish it as a real upstream dataset and take the graph edge.
If you need genuinely external, out-of-graph data, it belongs to a *different* pipeline.

### <a id="l10"></a>L10 🔵 An in-graph healing loop converges one update per correction round

Applies to `reconciliation_flows[].execution_mode: "pipeline"`
([`07_reconciliation_engine.md` §11](07_reconciliation_engine.md#11-execution-modes-job-pipeline-pipeline_audit_only)).
It is inherent, not a defect, and it is unchanged from job mode — but it surprises people who
expect an in-DAG healer to be self-correcting *within* the run.

The topological sort orders the ingestion read strictly **before** the reconciliation node that
produces the corrections — that ordering is exactly what makes the reconciliation source
same-update fresh — and the corrective append goes to a **sink**, which is outside the graph
entirely. So **this update's corrections cannot be consumed by this update's read.** The only shape
that would close the loop in-update is precisely the self-cycle [L1](#l1) rejects.

Read a `reconciliation_run_log` row as *"appended N rows; expect convergence on the next update,"*
not as *"the discrepancy is gone."* A dashboard that alerts on a non-zero
`missing_in_target_count` from the same update that healed it will alert on every healthy
correction round.

---

### <a id="l11"></a>L11 🔴 A `FULL_SNAPSHOT_CDC` flow is not in the source plane — reading it must bypass `bind()`

**Fixed in v1.6.1.** `engine/source_plane.py::_plan_ingestion_consumers` deliberately skips every
ingestion row whose `cdc_load_strategy` is `FULL_SNAPSHOT_CDC`, because
`apply_changes_from_snapshot`'s lambda reads a **path**, never a pipeline dataset
([L3](#l3)). The plan therefore holds no consumer under `"<dataflow_id>:source"`, and
`engine/source_plane.py::bind` never guesses:

```
FrameworkConfigError: source_plane.bind: unknown consumer_id 'df_sample_01_part_snapshot_ingest:source'.
Known consumer ids: ['df_sample_01_customer_scd1_ingest:source', ...]
```

The part that is easy to get wrong is that the exclusion applies to the **lambda**, not to the
whole flow. A snapshot flow still registers a staged view in the graph —
`_<t>_staged` → `_<t>_clean` → `_<t>_snapshot_input` → target — and that view needs a real source
read. Until v1.6.1, `engine/flow_generators.py::_build_ingestion_dataframe` called `bind()`
unconditionally, so **every** `FULL_SNAPSHOT_CDC` pipeline failed at graph analysis. It survived
review because the v1.6.0 source-plane work was never deployed; Sample 01 hit it on the first live
run (2026-09-01).

The generator now branches on `source_plane.SNAPSHOT_EXCLUDED_FROM_SOURCE_PLANE` — the single
exported constant both sides share, so the two can never disagree — and reads directly:

```python
if is_snapshot_cdc:
    staged_df = read_ingestion_source(spark, flow_row.source_type, source_config)
else:
    staged_df = bind(plan, source_consumer_id, want_stream=is_streaming)
```

That is a correct read-once outcome, not an exception to R2: the plane deduplicates a locator
*shared between consumers*, and a snapshot source has exactly one consumer by construction. Every
other strategy still binds — pinned in both directions by `tests/unit/test_flow_generators.py` §6.

**If you are adding a new CDC strategy** that also sits outside the plane, add it to the exported
constant and to the generator's branch together; a strategy excluded on one side only fails
nowhere until a real pipeline update.

## Deployment & operations

### <a id="o1"></a>O1 🔴 Never `bundle deploy` while a pipeline update is running

DABs prunes superseded artifacts from `<artifact_path>/.internal/`, and does so on a UC Volume
exactly as it does in the workspace. A deploy issued while a Lakeflow update is installing the
previous wheel kills it with `ENVIRONMENT_PIP_INSTALL_ERROR`.

Unique per-deploy wheel filenames prevent a wheel being **overwritten** in place, but not
**removed**. The operational rule stands regardless of filenames.

**Enforce it rather than remembering it.** `--fail-on-active-runs` refuses the deploy outright if
any job or pipeline in the bundle is running, turning a silent mid-run kill into a clean upfront
failure:

```bash
databricks bundle deploy -t <target> --fail-on-active-runs
```

**Keep a copy you can roll back to.** `scripts/archive_deployed_wheel.py`, run *after* a successful
deploy, copies the published wheel into `<artifact_path>/archive/`. DABs manages only `.internal/`,
so archived wheels are never pruned:

```bash
python scripts/archive_deployed_wheel.py --profile <profile>   # --prune-keep N to cap the archive
```

> ⚠️ The archive is **recovery, not prevention**. Deployed resources are pinned to the *absolute*
> `.internal/` path, so an archived copy does not repair a pin whose target was pruned mid-install —
> that means repointing the resource at the archived path by hand. `--fail-on-active-runs` is what
> stops the incident; the archive is what you fall back on if one happens anyway.

> 📌 A related claim that was **false and is now corrected**: `scripts/build_and_upload_wheel.py`
> used to state that a UC Volume never prunes. It does. Only files *outside* `.internal/` are safe.

### <a id="o2"></a>O2 🔴 DABs' sync snapshot goes stale — notebooks silently do not update

`.databricks/bundle/<target>/sync-snapshots/` tracks synced files and can go stale, reporting
`Files: 0 uploaded` even when local notebooks genuinely changed. Resources still redeploy, so the
run *looks* clean while the workspace keeps executing the **old** notebook.

**Fix:** `rm -rf .databricks/bundle/<target>/sync-snapshots` then redeploy.

**Always verify a notebook edit landed** before trusting a test result:

```bash
databricks workspace export <file_path>/notebooks/<dir>/<name> --format SOURCE -p <profile>
```

Note the deployed notebook path has **no `.py` extension** — exporting `<name>.py` returns
"Path doesn't exist", which reads exactly like an empty file if you pipe it to `grep -c`.

### <a id="o3"></a>O3 🟡 Flow retries default to 5 — a flaky flow reports SUCCESS

`pipelines.maxFlowRetryAttempts` defaults to **5** for triggered pipelines. A flow that fails
transiently is retried and the update still reports SUCCESS, so intermittent instability is
invisible in the run log.

For any run whose purpose is to **measure** stability, set it explicitly:

```yaml
configuration:
  pipelines.maxFlowRetryAttempts: "0"
```

and `max_retries: 0` on every job task. Retries are not attempted for ad-hoc editor updates or
Validate updates in any case.

### <a id="o4"></a>O4 🔵 Quota and concurrency limits produce failures that look like framework bugs

* **UC object quota** — roughly 50 schemas per catalog and 50 volumes per metastore on free tier.
  `QUOTA_EXCEEDED.UC_RESOURCE_QUOTA_EXCEEDED` fires in `seed_*` / `setup_control_tables` tasks,
  **before** any framework code runs.
* **Serverless compute quota** — `RESOURCE_EXHAUSTED: You've hit the limit for serverless compute
  for free usage`, triggered at concurrency 6; concurrency 3 is fine.
* **Shared pipelines** — two test cases sharing one Lakeflow pipeline, run concurrently, produce
  `Pipeline update already in progress`. Serialise them.

Check before blaming code: `databricks schemas list <catalog> -p <profile>` and
`databricks jobs list --active-only`.

### <a id="o5"></a>O5 🟠 A new control-table column never reaches an existing workspace by `CREATE TABLE IF NOT EXISTS`

Every statement in `control_plane/ddl_definitions.py::get_all_control_table_ddls` is a
`CREATE TABLE IF NOT EXISTS`, which is a **no-op against a table that already exists**. So a column
added to one of those `CREATE` statements reaches **new installations only**. An
already-provisioned workspace keeps the old table shape indefinitely, with nothing logged and
nothing to notice.

**What that looks like in practice.** Verified live: `flowx.config.reconciliation_flow_spec` had
**none** of `execution_mode` / `publish_schema` / `dq_config_json`, so pipeline-mode onboarding on
that workspace failed with `UNRESOLVED_COLUMN` on the column it was trying to write.

**The fix, and its operational catch.** `ddl_definitions.py` now carries
`ADDITIVE_CONTROL_TABLE_COLUMNS` (bare table name → `(column_name, sql_type, comment)`) plus
`get_add_column_ddl()`, and `schema_provisioner.py` gained `ensure_control_table_columns(...)`,
called at the end of `ensure_control_schema_exists`.

> **`databricks bundle deploy` does NOT apply this migration.** Only **running** the
> `setup_control_tables` task (`notebooks/01_setup/01_setup_control_tables.py`) does. A deploy that
> "succeeds" leaves the control tables exactly as they were, and the next onboarding still fails
> with `UNRESOLVED_COLUMN`. Deploy, then run `setup_control_tables`, then onboard.

Three properties worth knowing before you extend it:

* **Strictly additive.** `ALTER TABLE ... ADD COLUMNS` only — never a drop, never a retype. Every
  existing row's new column is left NULL, which is exactly what each column's DDL comment documents
  as its default (`execution_mode` NULL means `"job"`, so an already-onboarded flow keeps its
  current behaviour rather than silently switching modes).
* **Not `ADD COLUMNS IF NOT EXISTS`.** Databricks SQL rejects that spelling with
  `PARSE_SYNTAX_ERROR` — verified live. Idempotence is caller-side (skip columns already present)
  plus a narrow duplicate-column race swallow (`_is_duplicate_column_race`), the same shape as
  [L6](#l6).
* **On a brand-new workspace the migration is a no-op**, because the columns are in the `CREATE`
  DDL too. It matters only for workspaces provisioned before the column was added.

### <a id="o6"></a>O6 🟠 A pipeline reading another target's `artifact_path` is orphaned by every deploy of that target

A Lakeflow pipeline installs the framework wheel from an `artifact_path`. Each `bundle deploy`
builds a **new, uniquely-named** wheel, **prunes** the previous one (see [O1](#o1)), and updates
only the pipelines **that target owns**.

A pipeline that reads its wheel from another target's (or another principal's) artifact path is
therefore left pointing at a wheel that no longer exists, and fails with:

```
ENVIRONMENT_PIP_INSTALL_ERROR
```

**No deploy of that target repairs it** — the deploy that broke it does not know the pipeline
exists, and a deploy of the pipeline's own bundle is not what is running. Unique per-deploy wheel
filenames prevent overwrite-in-place, not removal.

**Live instance, currently unresolved.** Pipeline `e41a47ba-5ad0-4dc5-9535-5aa16cc97e65` is a
`[dev arjun]` pipeline reading from `flowx@nrmanalytix.com`'s artifact path, so every
`dev_flowx` deploy orphans it. It needs its own `artifact_path`, or to be brought under the
bundle. This is a **pre-existing bundle-topology problem**, not a consequence of any framework
release.

**A second, independent blocker on that same scenario, also unresolved.** Its reconciliation target
`flowx.bronze_excalibur.bronze_tariffelementband` grants `SELECT` to `arjun@`, `gowtham@` and
`varadaraju@` only — **not** to `flowx@nrmanalytix.com`, which is the identity the pipeline runs
as. Schema-level access is fine (the same identity reads `bronze_excalibur.autoload_bronze`), so
this is a **table-level grant gap**:

```sql
GRANT SELECT ON TABLE flowx.bronze_excalibur.bronze_tariffelementband TO `flowx@nrmanalytix.com`;
```

Until both are cleared, that scenario is verified **offline only** — validator plus
`plan_source_plane`, pinned by `tests/unit/test_geneva_e41a47ba_topology.py` — and has never been
confirmed by a live pipeline run. See [R7](#r7).

### <a id="o7"></a>O7 🔴 The v1.6.0 upgrade renames and unpublishes intermediates — plan it per pipeline

v1.6.0's Intermediate Object Rule changes the **identity** of several datasets an already-deployed
pipeline has been maintaining. On the first update after deploying the v1.6.0 wheel:

* A **materialized `_<target>_staged` intermediate** (quarantine flows, `sink`/`external_sink`
  flows) stops being the published `catalog.schema._<target>_staged` table and becomes a temporary
  table under its bare name. Lakeflow treats that as a **new dataset**: the old published table is
  removed from the pipeline's managed set, and — for a streaming flow — the staged intermediate's
  **checkpoint state resets**, so its Auto Loader/stream read starts over. For an `APPEND` final
  target that can mean **re-ingesting rows it already appended**; SCD/apply-changes targets dedup by
  key and self-heal.
* **L0 source-plane nodes** (`_src__…__stream/batch`) similarly vanish from Unity Catalog unless the
  spec explicitly sets `source_plane.catalog` + `source_plane.schema`, and their stream state resets
  under the temporary re-registration.
* **Reconciliation** L3/L4 plumbing is renamed/unpublished (`recon__…__classified` →
  `_recon__…__classified`, temporary), and `__metrics`/`__mismatch` exist only when their capture
  flags are on — see [`07` §11.10](07_reconciliation_engine.md#1110-v160--the-intermediate-object-rule-and-the-conditional-audit-datasets).

**What to do, per pipeline, before the first post-upgrade update on anything that matters:**

1. Inventory external consumers of the disappearing tables (`_staged`, `_src__*`,
   `recon__*__classified`) and repoint them — `__mismatch`/`__metrics` for recon consumers; for a
   shared source node, set `source_plane.catalog`/`schema` to keep it published.
2. Expect the update to behave like a structural change: prefer a maintenance window, and for
   streaming `APPEND` flows decide deliberately between accepting a one-time re-ingest window or a
   coordinated full refresh with the landing zone quiesced.
3. The old published intermediate tables that Lakeflow does not clean up itself can be dropped once
   the first v1.6.0 update has succeeded — they are orphans, no longer maintained by anything.

**None of this affects a freshly-onboarded pipeline** — only ones that ran under ≤ v1.5.x. The
sample suite (`resources/sample_jobs/`) was born on v1.6.0 and is unaffected.

---

### <a id="o8"></a>O8 🔴 A module loaded from a job task must not transitively `import dlt`

**Fixed in v1.6.1.** `import dlt` is not free outside a Lakeflow pipeline. It calls into the
runtime's notebook entry point, which in a plain **job** notebook task has no notebook id to
return, and the import dies before any framework code runs:

```
Py4JJavaError: An error occurred while calling o36.get.
: java.util.NoSuchElementException: None.get
    at scala.None$.get(Option.scala:627)
```

Confirmed live 2026-09-01. `observability/reconciliation_export.py` imported one *pure*
exception-inspection helper (`_is_table_not_found`) from `dq/quarantine.py`, which does a
module-level `import dlt`. Sample 03's `observability_export` task failed at **import time** and
took its downstream `store_sample_config` with it — while the pipeline that produced the
reconciliation datasets had succeeded minutes earlier, which is what makes the failure so
confusing to read. `reconciliation/appender.py` carried the identical latent import and would
have broken the standalone reconciliation job the same way.

**Transitivity is the whole trap.** The diff of `reconciliation_export.py` shows a helper import
from a sibling module and nothing alarming; the `dlt` edge is one hop further out. The fix is not
to move the import site but to give shared code a home that cannot drag `dlt` in:
`dq/table_errors.py` now holds `is_table_not_found` / `TABLE_NOT_FOUND_CONDITIONS` and imports
**nothing at all**. `dq/quarantine.py` keeps `_is_table_not_found` as an alias for pipeline-side
callers — importing *that alias* from job context reintroduces the bug in full.

**It does not fail locally.** `databricks-dlt` is a dev dependency, so `import dlt` succeeds under
pytest and tells you nothing about the serverless job runtime. The guard is therefore static:
`tests/unit/test_job_context_has_no_dlt_import.py` walks the import graph with `ast` and asserts
the edge is absent, using `dq/quarantine.py` itself as a control so the assertions cannot pass
vacuously.

Job-context entry points to keep clean:

| Module | Loaded by |
|---|---|
| `observability/reconciliation_export.py` | `notebooks/08_observability/08_dlt_observability_engine.py` |
| `reconciliation/appender.py` | `notebooks/05_reconciliation/05_reconciliation_engine.py` |
| `dq/table_errors.py` | shared — must stay importable from anywhere |

---

### <a id="o9"></a>O9 🔴 A `rate`-source pulse is a race — the `per_update` export trigger uses `rate-micro-batch`

Spark's plain `rate` source counts rows as wall-clock seconds since its checkpoint was created, so
under a triggered update's `AvailableNow` a one-row pulse is nondeterministic. Measured live:

| Pulse source | Fresh checkpoint | Incremental update |
|---|---|---|
| `rate` (`rowsPerSecond=1`, `.limit(1)`) | 1 row | **0 rows** |
| `rate-micro-batch` (`rowsPerBatch=1`) | 1 row | 1 row |

UC6's first green update had correct gold tables and **four empty sinks** because the pulse emitted 0
rows. The shipped trigger uses `rate-micro-batch` (`rowsPerBatch=1`, no `.limit()`), which is
wall-clock independent. Related trap: streaming flows never report `num_output_rows` in
`event_log()` — a zero-row streaming flow is invisible there; check the sink's output.

### <a id="o10"></a>O10 🔴 Both validation gates pass and the update still fails — graph planning is a third gate

Neither `spec_validator.py` nor the JSON schema plans the Lakeflow graph. Runtime-only defects
surface one per ~10-minute update. UC6 hit five: G-STREAM (sink streaming from an aggregating MV);
`path` pointing at a directory nothing writes (`CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE`); unuploaded
`schema_config_path` files; `match_keys: ["__framework_hash_key"]` (circular — the matcher hashes
real columns into it); a streaming/batch `UNION ALL` and a `ROW_NUMBER()` over a stream. Pre-run
checklist and the governance-on-`sink` corollary: `agent_skills/reference/common_pitfalls.md` §43.

## Open discrepancy — verify before relying on either statement

**Normalization vs schema_config ordering.** Two sources in this repo disagree:

* `ingestion/column_normalization.py`'s module docstring states normalization *"must run
  immediately after the raw source read — **before** `ingestion.schema_config`'s renames."*
* `notebooks/03_engine/03_lakeflow_declarative_pipeline.py` runs `apply_schema_config` **first**,
  then `normalize_column_names(df, source_config)` (the function; the *config* key is
  `column_normalization`).

The notebook is what executes, and [C2](#c2) documents that order. The docstring is what an
operator reads while writing a spec. Until this is resolved, **write `schema_config` entries
against the source's raw column names and every other field against the normalized names**, which
is correct under the code as it stands.

---

## Related documents

* [`00_master_reference_index.md`](00_master_reference_index.md) — every JSON attribute: type, required, default, allowed values
* [`12_module_permutation_matrix.md`](12_module_permutation_matrix.md) — which combinations are legal
* [`02_ingestion_and_sources.md`](02_ingestion_and_sources.md) · [`03_transformation_and_cdc.md`](03_transformation_and_cdc.md) · [`04_data_quality_and_governance.md`](04_data_quality_and_governance.md) · [`05_security_and_cryptography.md`](05_security_and_cryptography.md) · [`06_egress_and_lakeflow_sinks.md`](06_egress_and_lakeflow_sinks.md) · [`07_reconciliation_engine.md`](07_reconciliation_engine.md) · [`08_observability_and_telemetry.md`](08_observability_and_telemetry.md)
* [`11_hashing_and_determinism.md`](11_hashing_and_determinism.md) — the v1.3.0 hash migration checklist
* [`09_developer_guide_and_recipes.md`](09_developer_guide_and_recipes.md) — the 11-step build walkthrough
