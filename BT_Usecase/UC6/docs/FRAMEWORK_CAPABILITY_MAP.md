# FlowX Framework Capability Map — UC6 Flood Warning System

Companion to [`BUILD_CONTRACT.md`](BUILD_CONTRACT.md). Every row was resolved by reading the
framework's own source, not by recall. The authority for allowed attributes is
`src/flowx/lakeflow_framework/onboarding/spec_validator.py` (its `ALLOWED_*` constants), which
`tests/unit/test_unknown_key_rejection.py` asserts against the JSON schema.

> **Unknown attributes are a hard error since v1.7.1.** A plausible-looking key is worth nothing;
> every spec must validate before it is handed over.

---

## 1. The capabilities UC6 needs

| # | Capability | Verdict | FlowX config key |
|---|---|---|---|
| 1 | Glob/pattern filename matching | **Config — already existed** | `source_config.file_pattern` → Spark `pathGlobFilter` |
| 2 | Pipe (and comma) field delimiter | **Config — already existed** | `source_config.reader_options.delimiter` |
| 3 | Explicit column names for headerless files | **Config — already existed** | `source_config.schema_config_path` → external JSON, `source_name: "_c0"` |
| 4 | Archive originals after ingest | **Config — already existed** | `source_config.landing_retention_policy.clean_source: "archive"` + `archive_path` |
| 5 | Table & column tagging | **Config — already existed** | `governance_tags.table_tags` / `.column_tags[]` |
| 6 | Multi-way joins, unions, rollups, CASE rules | **Config — already existed** | `transformation_flows[].transformation_sql` + `source_inputs[]` |
| 7 | Configurable thresholds | **Config — already existed** | `pipeline_parameters` + `${param}` in SQL |
| 8 | Fail on missing/empty source file | **Config — already existed** | reconciliation `dq_config` on `source_record_count > 0` (see §3) |
| 9 | **Symmetric (passphrase) PGP** | **CODE — F1, new** | `crypto/pgp.py::pgp_{encrypt,decrypt}_symmetric` |
| 10 | **gzip landing members + symmetric pre-decrypt** | **CODE — F2, new** | `source_zip_handling.member_format: "gzip"`, `pre_extraction_decryption.type: "pgp_symmetric"` |
| 11 | **gzip egress, CSV dialect, symmetric PGP on sink** | **CODE — F3, new** | `sink_config.staged_file_options`, `post_export_archive.archive_format: "gzip"`, `pgp_encryption.passphrase_secret` |

**Three of the four enhancements the brief proposed in its §7 already existed** (rows 1, 2, 4 and
the tagging helper). The three that genuinely did not exist (rows 9–11) were **not** in the brief's
list. They are committed separately, ahead of any UC6 configuration:

| Commit | Scope |
|---|---|
| `9698013` | F1 — symmetric PGP encrypt/decrypt |
| `348ca11` | F2 — gzip landing members + `pgp_symmetric` pre-extraction |
| `98429e1` | F3 — gzip archives, CSV dialect, symmetric PGP on the sink |

---

## 2. Deviations between the supplied brief and the framework's actual behaviour

Each is a place the request cannot be met literally. In every case the repo's behaviour wins, per
the brief's own §0 instruction — but it is called out rather than silently absorbed.

### 2.1 Asset naming puts the number first

The brief asks for `007_uc6_lfj_EA` / `008_uc6_ldp_EA`. Every existing asset is
`NNN_lfj_<name>` / `NNN_ldp_<name>`, lowercase: `001_lfj_uc7_cdr_asn`,
`004_ldp_uc3_excalibur_streaming_cdc`. **Adopted:** `007_lfj_uc6_ea_flood_warning` /
`008_ldp_uc6_ea_flood_warning`. 007 and 008 are confirmed free (001 = UC7, 003–006 = UC3).

### 2.2 `observability.appl_logs` does not exist

The brief and the supplied design both route logging to `observability.appl_logs`. That table
appears in **no framework code** — only in the UC6 design document itself. The framework's real
surfaces are the structured JSON logger (driver stdout), Lakeflow's native event log, and a
configurable `DATABRICKS_VOLUME` / `OTLP_CONSUMER` export destination. **Adopted:** volume export
to `/Volumes/br_digital_poc/observability/app_logs/uc6/`, matching UC3's existing use of that volume. See
§4.

### 2.3 There is no tagging taxonomy to conform to

The brief says "if the repo already has a tagging taxonomy, use it instead". It does not:
`table_tags` and `column_tags[].tags` are free-form `string→string` maps validated only for being
strings. The brief's proposed tag set is adopted verbatim.

### 2.4 Ingestion is always streaming

`source_plane.py` rejects a batch bind of an ingestion source outright — every `source_type` is
unconditionally a `spark.readStream`, and there is no batch file reader. UC6's weekly batch runs as
a **triggered** (`continuous: false`) update over Auto Loader, exactly as UC7 and UC3 do. The sink
additionally requires a streaming upstream (`require_streaming_source()`).

### 2.5 The DQ gate cannot live on the ingestion flow

The brief's §5.1 asks for a DQ expectation asserting `file_count > 0 AND row_count > 0` per source.
An ingestion `dq_config` rule is a **per-row** boolean predicate evaluated inside `dlt.expect_*`.
Zero rows means zero evaluations, so an emptiness rule attached there can never fire — a missing
file would pass silently, which is the exact failure the requirement exists to prevent.

**Adopted:** one reconciliation flow per required source, asserting `source_record_count > 0` with
action `fail` on the one-row `__metrics` dataset, where that count is a real column. This fails the
update and writes a row to the reconciliation control tables. *File* count is not separately
assertable and is not independently meaningful: a missing file and an empty file both yield zero
rows, and both must fail.

---

## 3. Findings about the supplied sample data

Verified by decompressing and field-counting every file. **These contradict the brief and are the
reason the acceptance tests needed an augmented fixture.**

### 3.1 Excalibur is comma-delimited, not pipe

The brief states pipe is the delimiter "inside every `.dat`/`.csv` file". `CM_EXCALIBUR_ADDRESS`
has 43 comma-separated fields and **no pipes at all**. Configuring it as pipe yields a
single-column table and silently breaks the whole Excalibur path.

### 3.2 The EA file has a header row; the other five do not

`reader_options.header` is `"true"` for EA and `"false"` for the rest, which is why the five
headerless sources need `schema_config_path` files mapping positional `_c0.._cN`.

### 3.3 `CSS_account_*.dat.gz` also matches `CSS_account_address_*.dat.gz`

The two files have different schemas (56 vs 19 fields), so the obvious glob would feed both into
one table and corrupt both. **Adopted:** `CSS_account_[0-9]*.dat.gz`, which the address filename
cannot match. Verified against the real filenames.

### 3.4 The fixture produces ZERO matches — two independent reasons

| Check | Result |
|---|---|
| EA postcodes vs **any** EE source postcode | **0 overlap** (EA: `AB12 3CD`, `EF45 6GH`, `IJ78 9KL`, `MN10 2OP`; CSS: `RH2 9QQ`; Excalibur: `HA61BL`…; JT: `TN393QN`…) |
| CSS account ⋈ subscription ⋈ address, on every candidate id column | **0 overlap** (account ids ~2873–3051, subscription ~3296–3715, address ~46649141) |

The three CSS files were evidently generated independently, so the CSS path yields zero rows even
before postcodes are considered. Consequently **the brief's acceptance test §9.3 — "spot-check
OSAPR STATUS values (Found / Not Found / Bad OSAPR / Single Addr)" — cannot pass on the bundle as
shipped**: every EA row would land as `Bad OSAPR` or `Single Addr`.

**Adopted:** the supplied bundle is kept intact and unmodified as the as-supplied reference case,
and an additional generated fixture exercises all four status branches plus the telephone privacy
rule. Both are run.

### 3.5 Match strength is undefined in every supplied document

The legacy Ab Initio scoring algorithm is in none of the source material. Implemented as an
explicit, documented, parameterised SQL expression (`BUILD_CONTRACT.md` §6) confined to a single
`transformation_sql` so it can be swapped wholesale when the real algorithm surfaces. **This is a
stated assumption, not a recovered requirement.**

---

## 4. Observability — where UC6 events actually land

| Surface | What it carries | How to read it |
|---|---|---|
| Structured JSON logs | One JSON line per flow operation: flow id, rows read/written/rejected/quarantined, status, duration, errors | Pipeline update's driver log |
| Lakeflow event log | Per-dataset flow progress and DQ expectation pass/fail counts, natively | `event_log(:pipeline_id)` TVF |
| Volume export | The above, exported as `.jsonl.gz` by the job's `observability_export` task | `/Volumes/br_digital_poc/observability/app_logs/uc6/` |
| Reconciliation control tables | The §2.5 emptiness gate's own result rows | `br_digital_poc.config.reconciliation_run_log` etc. |

The structured logger deliberately does **not** write into Lakeflow's event log: that table has a
fixed, closed set of `event_type` values and no documented API to append an application-defined
event. Claiming otherwise would overclaim a capability Lakeflow does not expose.
