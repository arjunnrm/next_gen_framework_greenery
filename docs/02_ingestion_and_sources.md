# 📥 FlowX — Ingestion & Source Reader Engine

> **Audience**: Data engineers onboarding raw data into Bronze Delta tables using Auto Loader, streaming message buses, or binary protocols.

---

## 1. Overview of Ingestion Capabilities

FlowX provides native, configuration-driven source readers for three primary ingestion modalities:
1. **`autoloader`**: High-throughput cloud object storage ingestion (JSON, CSV, Parquet, Avro, Text, Binary) powered by [Databricks Auto Loader (`cloudFiles`)](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/index.html).
2. **`zerobus`**: Low-latency event streaming from Zerobus / Kafka / Delta streaming tables using Spark Structured Streaming.
3. **`asn1`**: High-performance telecom CDR (Call Detail Record) decoding supporting ASN.1 BER and DER binary encodings via partitioned `mapInPandas`.

---

> **v1.5.0 — every source read now goes through the read-once source plane.** A physical table
> or path is read **once per pipeline update** and shared by all its consumers, instead of each
> consumer opening its own read. Two consequences worth knowing before you configure a source:
>
> * An Auto Loader path with two quarantine rules used to open **two** `cloudFiles` streams over
>   that path, sharing one `cloudFiles.schemaLocation`. The `_<target>_staged` view is now
>   materialized whenever it has more than one consumer (quarantine rules, or a `sink` /
>   `external_sink` target), so the source is read once and the stream is opened once.
>   **v1.6.0:** that materialization is a pipeline-scoped `@dlt.table(temporary=True)` under the
>   bare `_<target>_staged` name — materialized once per update but never published to Unity
>   Catalog (the Intermediate Object Rule; before v1.6.0 it was a published, fully-qualified
>   `catalog.schema` table). Downstream readers resolve it by the same pipeline-local name either
>   way. If an existing pipeline's catalog shows a `_<target>_staged` table today, the first
>   update after v1.6.0 unpublishes it — see
>   [`13_known_limitations_and_gotchas.md` O7](13_known_limitations_and_gotchas.md#o7) before
>   upgrading a deployed pipeline.
> * File-lifecycle side effects — `landing_retention_policy` (`cloudFiles.cleanSource` *moves or
>   deletes* committed landing files) and `source_zip_handling` (PGP-decrypt, unzip,
>   `.__framework_extracted__` markers) — therefore run **exactly once** per update. Two
>   *different* lifecycle policies on the same path are now rejected at onboarding and again at
>   plan time, because two competing regimes on one directory is a data-loss bug.
>
> Sharing is keyed on base-read options only (format, `schema_location`, `file_pattern`,
> `reader_options`, retention/ZIP policy, …); everything applied *after* the read — `schema_config`,
> `column_normalization`, `explode_columns`, `remove_dups`, encryption — is a per-consumer overlay
> and does **not** split the read. Since v1.6.0 a shared `_src__…` plane node is likewise a
> pipeline-scoped **temporary** table unless the spec sets **both** `source_plane.catalog` and
> `source_plane.schema` (null no longer falls back to the pipeline's own catalog/schema). Full
> mechanics:
> [`01_platform_architecture.md` §7](01_platform_architecture.md#7-the-read-once-source-plane).

---

## 2. Auto Loader Ingestion (`source_type: "autoloader"`)

Auto Loader incrementally and efficiently processes billions of new files arriving in Unity Catalog Volumes or cloud storage (`s3://`, `abfss://`, `gs://`).

### Configuration Schema
```json
{
  "source_type": "autoloader",
  "source_config": {
    "path": "/Volumes/{{catalog}}/landing/raw_orders",
    "format": "csv",
    "schema_location": "/Volumes/{{catalog}}/landing/checkpoints/orders",
    "schema_evolution_mode": "addNewColumns",
    "file_pattern": "orders_*.csv",
    "reader_options": {
      "header": "true",
      "delimiter": ",",
      "maxFilesPerTrigger": "1000"
    },
    "capture_technical_metadata": true,
    "landing_retention_policy": {
      "clean_source": "archive",
      "archive_path": "/Volumes/{{catalog}}/landing/archive/orders",
      "retention_days": 7
    }
  }
}
```

### Schema Evolution Policies
FlowX supports all native Auto Loader schema evolution modes via `schema_evolution_mode`:
- `"addNewColumns"` (Default): Automatically appends newly discovered columns to the Bronze Delta table.
- `"addNewColumnsWithTypeWidening"`: Appends new columns and widens compatible types (e.g. `INT` to `BIGINT`).
- `"rescue"`: Directs unparseable rows or unknown columns into a dedicated `_rescued_data` column without failing the stream.
- `"failOnNewColumns"`: Immediately stops ingestion if new columns are detected.
- `"none"`: Disables Auto Loader's schema evolution handling entirely.

### Landing Retention Policy (`landing_retention_policy`)

`source_config.landing_retention_policy` maps directly to Auto Loader's own `cloudFiles.cleanSource*` options and governs what happens to a landing file **after** Auto Loader has ingested it — it never touches files before they're read (see the invariant callout below for what does).

| Field | Type | Default | Notes |
|---|---|---|---|
| `clean_source` | string | `"off"` | One of `"archive"`, `"delete"`, `"off"`. |
| `archive_path` | string | none | Only meaningful for `clean_source: "archive"` — **never required**, even then. See below. |
| `retention_days` | integer | **`7`** | `>= 0`. `0` is legal and means "no age threshold" — a file becomes eligible for archive/delete the moment Auto Loader commits it. |

Three behaviors worth calling out precisely, since each is a deliberate design choice rather than an obvious default:

- **Omitting `retention_days` now means 7 days, explicitly** (`DEFAULT_LANDING_RETENTION_DAYS` in `ingestion/readers.py`) — not "whatever Auto Loader's own untouched default is." A spec that relied on that implicit behavior must now set `retention_days` explicitly to get the same effect.
- **`clean_source: "delete"` ignores `archive_path` entirely.** Deleting has no destination, so a stray `archive_path` left behind from an earlier `"archive"` configuration is silently ignored, not flagged as an error — an unused sibling field is not a misconfiguration.
- **`clean_source: "off"` (explicit, or degraded — see below) emits no `cloudFiles.cleanSource*` option at all.** `OFF` is already Auto Loader's own default, so omitting the option entirely is byte-for-byte equivalent, while avoiding passing a runtime an option it may not need to accept.

> [!IMPORTANT]
> **`clean_source: "archive"` with an empty or absent `archive_path` degrades to `"off"` — this is a documented no-op, not a validation error.** It used to hard-fail the pipeline update; as of v1.3.0 it doesn't, because blanking `archive_path` is the one edit an operator naturally reaches for to *temporarily* pause archiving without deleting the whole `landing_retention_policy` block and losing its other settings. The framework logs a `WARNING` instead of raising:
> ```
> landing_retention_policy.clean_source='archive' but archive_path is empty/absent -- degrading to clean_source='off': NO retention or cleanup action will be taken for this source.
> ```
> This is legal and takes **no action of any kind**:
> ```json
> { "landing_retention_policy": { "clean_source": "archive", "archive_path": "" } }
> ```

> [!NOTE]
> **`landing_retention_policy` is an Auto Loader (`cloudFiles`) concern only — it is never applied to `source_zip_handling` or the raw ZIP pre-extraction path.** It is legal on `source_type: "autoloader"` and `source_type: "asn1"` (both are `cloudFiles` readers). Configuring it on `source_type: "zerobus"` — which streams an existing Delta table and has no landing zone at all — is rejected at onboarding time:
> ```
> <path>.landing_retention_policy: only applicable to source_type 'autoloader'/'asn1' (Auto Loader file ingestion), but this flow's source_type is 'zerobus'
> ```
> The two archive lifecycles are kept strictly separate on purpose: `source_zip_handling.delete_source_after_extract` (§5–§6 below) governs the *raw, pre-extraction* archive, while `landing_retention_policy` governs the *already-extracted* files Auto Loader itself reads. Conflating the two would give one spec block two unrelated meanings and could delete an operator's only copy of an archive that hasn't been ingested yet.

---

## 3. Streaming Event Bus (`source_type: "zerobus"`)

For Kafka topics, event brokers, or Delta streaming sources, `zerobus` ingests real-time record streams with micro-batch checkpointing.

```json
{
  "source_type": "zerobus",
  "source_config": {
    "source_catalog": "{{catalog}}",
    "source_schema": "staging",
    "source_table": "zerobus_transactions_stream",
    "starting_version": "latest",
    "capture_technical_metadata": true
  }
}
```

---

## 4. ASN.1 Binary Decoding (`source_type: "asn1"`)

Telecom Call Detail Records (CDRs) and industrial telemetry often arrive as ASN.1 binary streams encoded in BER (Basic Encoding Rules) or DER (Distinguished Encoding Rules).

### High-Performance Partitioned Architecture
FlowX compiles ASN.1 schemas **once per Spark partition** using `mapInPandas`, eliminating the severe performance bottleneck of compiling schemas per record:

```
┌─────────────────────────────────────────────────────────────┐
│                    SPARK BINARY STREAM                      │
│       Binary CDR payload files read via Auto Loader         │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│             mapInPandas Partition Decoder                   │
│ • Compiles ASN.1 schema (.asn) once per worker partition    │
│ • Decodes binary chunks into structured Python dictionaries │
│ • Emits decoded JSONL DataFrame to Spark runtime            │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│              BRONZE DECODED STREAMING TABLE                 │
└─────────────────────────────────────────────────────────────┘
```

### Configuration
```json
{
  "source_type": "asn1",
  "source_config": {
    "path": "/Volumes/{{catalog}}/telecom/raw_cdr_binaries",
    "asn1_schema_path": "/Volumes/{{catalog}}/telecom/schemas/gsm_cdr.asn",
    "asn1_codec": "ber",
    "asn1_pdu_name": "CallEventRecord",
    "capture_technical_metadata": true
  }
}
```

### Choosing `asn1_pdu_name` on a real telecom module

The Spark output schema is derived from the `.asn` file itself
(`asn1/decoder.py::derive_asn1_field_defs`, via `asn1tools.parse_files` introspection) — there is
no hand-authored field list to keep in sync. Two constraints follow from that, and they are the
whole difficulty of onboarding a genuine module:

1. **The PDU must be a top-level `SEQUENCE`.**
2. **`CHOICE` is rejected anywhere in that `SEQUENCE`'s resolved member tree** — not merely at the
   top — as is a recursive/self-referential type.

Toy schemas hide this. `sample_data/asn1_schema/gsm_cdr.asn` is a single flat 5-field `SEQUENCE`,
so its PDU "just works". Every real telecom module is the opposite shape: a root `CHOICE` selecting
between message kinds. In `flowx_testing/BT_Testing/TAP.310.asn1` — the genuine GSMA TAP release
3.10 specification, 375 types — the module's own top-level `DataInterChange` is
`CHOICE { transferBatch, notification }` and is rejected outright; `TransferBatch` *is* a `SEQUENCE`
but reaches `CallEventDetail`, also a `CHOICE`, and is rejected one level deeper. `Notification`
(`[APPLICATION 2] SEQUENCE`) resolves completely, and is a real TAP3 file-level PDU — the
notification file a roaming partner sends when it has no chargeable events to transfer. That is
what [Sample 06](09_developer_guide_and_recipes.md#64-sample-06-the-real-gsma-tap3-module-and-why-the-pdu-is-notification)
ingests.

Rather than reading the module, ask the resolver which PDUs it can actually derive — on TAP.310
this reports 70 usable types out of 93 top-level `SEQUENCE`s in about a second:

```python
import asn1tools
from flowx.lakeflow_framework.asn1.decoder import derive_asn1_field_defs

module = "flowx_testing/BT_Testing/TAP.310.asn1"
types = next(iter(asn1tools.parse_files([module]).values()))["types"]
for name, node in types.items():
    if node.get("type") != "SEQUENCE":
        continue
    try:
        print(f"{name:40s} {len(derive_asn1_field_defs(module, name))} fields")
    except Exception:
        pass          # CHOICE / recursion / unresolvable member somewhere in the tree
```

Nested constructs are **not** flattened into columns: a nested `SEQUENCE`/`SET` becomes a Spark
`struct`, a `SEQUENCE OF`/`SET OF` an `array`, `ENUMERATED` its symbolic name as a string, and
`BIT STRING` a `struct<bytes, bit_length>`. Flatten downstream with `explode_columns` if a spec
wants columns. The module file must also define exactly **one** ASN.1 module; `IMPORTS` spanning
files is not supported.

Per-row decode failures never abort the micro-batch — they populate `_asn1_decode_error` and leave
every decoded column NULL, which is what makes `"expression": "_asn1_decode_error IS NULL"` with
`"action": "quarantine"` the standard first DQ rule on an `asn1` flow.

---

## 5. In-Flight PGP Decryption & ZIP Archive Handling

FlowX can transparently decrypt PGP-encrypted files and extract ZIP archives before passing raw payloads to the downstream reader (`autoloader` or `asn1`). This all runs inside the reader function itself — at the pipeline's actual *execution* time, not merely graph-definition time — so decrypt → unzip → Auto Loader ingest → downstream transforms happen within one Lakeflow Declarative Pipeline update, with no separate job task required.

```json
{
  "source_config": {
    "path": "/Volumes/{{catalog}}/landing/secure_archives",
    "source_zip_handling": {
      "enabled": true,
      "source_zip_path": "/Volumes/{{catalog}}/landing/incoming_zips",
      "zip_file_pattern": "*.zip",
      "target_volume_path": "/Volumes/{{catalog}}/landing/secure_archives",
      "pre_extraction_decryption": {
        "type": "pgp",
        "private_key_secret": {
          "secret_catalog": "poc",
          "secret_schema": "security",
          "secret_key": "pgp_private_key"
        },
        "passphrase_secret": {
          "secret_catalog": "poc",
          "secret_schema": "security",
          "secret_key": "pgp_passphrase"
        }
      },
      "delete_source_after_extract": { "action": "delete_now" }
    }
  }
}
```

`source_zip_handling` fields:

| Field | Type | Required | Notes |
|---|---|---|---|
| `enabled` | boolean | yes | No-op when `false`/absent. |
| `source_zip_path` | string | yes (when enabled) | A landing **directory** — never a single file. A real landing zone routinely accumulates more than one archive between pipeline updates (one per source extract/drop). |
| `zip_file_pattern` | string | yes (when enabled) | A glob selecting which archive(s) in `source_zip_path` this update processes, e.g. `"orders_*.zip"` or `"*.zip"`. Matched with `fnmatchcase` — case-sensitive, matching the Linux runtime Databricks compute actually runs on, regardless of what OS last edited the spec. |
| `target_volume_path` | string | yes (when enabled) | Extraction destination — typically the same path the downstream `autoloader`/`asn1` reader's `path` points at. |
| `member_format` | string | no | **v1.7.4.** `"zip"` (default — the only pre-v1.7.4 behaviour) or `"gzip"`. Selects the archive **container**, orthogonally to any decryption layer. See below. |
| `pre_extraction_decryption` | object | no | Fully optional at every level — absent or `{}` means a plain, unencrypted, non-password-protected ZIP. See below. |
| `delete_source_after_extract` | boolean \| object | no | Default `{"action": "delete_now"}`. See the subsection immediately below. |

> [!WARNING]
> The correct field names are **`source_zip_path`**, **`zip_file_pattern`**, and **`target_volume_path`**. `file_pattern` is a *different*, top-level `source_config` field (§2) that maps to Spark's generic file-source `pathGlobFilter` — it has no effect inside `source_zip_handling` and naming it there silently matches nothing.

`pre_extraction_decryption` — two independent, combinable concerns, both optional:
- **`type: "pgp"`** — decrypts the whole file with a PGP private key (`private_key_secret`, required; `passphrase_secret`, optional — a real, properly-secured PGP private key is routinely passphrase-protected) *before* it is treated as a ZIP at all. Omit `type` entirely when the archive has no outer PGP layer.
- **`secret_passphrase`** — the AES-256 password on the ZIP archive itself, resolved independently of `type`. Present with no `type` means "just a password-protected ZIP." Both present means "decrypt the PGP envelope first, then extract the password-protected ZIP it contained."
- **`type: "pgp_symmetric"`** (v1.7.4) — decrypts a **passphrase**-encrypted OpenPGP message, the shape `gpg --symmetric --cipher-algo AES256` produces. `passphrase_secret` is **required** here (it is the entire secret, not merely a key unlock), and `private_key_secret` is **rejected**. It is a distinct `type` rather than an option on `"pgp"` because the two are mutually exclusive at the message level: a key-encrypted message is not passphrase-decryptable, and vice versa.

#### `member_format`: ZIP archives vs. gzip streams (v1.7.4)

Everything above assumes a ZIP — a container holding N *named* members, opened by `pyzipper`. A gzip file is not that: it is a single compressed stream with no member table, and `pyzipper` cannot open one at all.

| `member_format` | When to use it |
|---|---|
| `"zip"` (default) | Any `.zip`, with or without an AES password, with or without an outer PGP envelope. Unchanged behaviour. |
| `"gzip"` | An **encrypted** gzip file, e.g. `.csv.gz.gpg` — decrypt the envelope, then decompress the bare gzip stream it contained. |

> [!IMPORTANT]
> An **unencrypted** `.gz` needs no `source_zip_handling` at all. Spark's Hadoop codec layer decompresses it transparently on read, so configuring a handling block for one buys nothing but a staging copy. `member_format: "gzip"` exists for the encrypted case, where the file must be decrypted to disk before anything can read it.

`secret_passphrase` is **rejected** for `"gzip"` — a gzip stream has no archive password, and accepting one would let a spec assert protection that nothing applies.

The landed filename has its `.gz` **and** any `.gpg`/`.pgp` envelope suffix stripped, so `EE_2026-08-20-REQUEST_1OF1.csv.gz.gpg` lands as `EE_2026-08-20-REQUEST_1OF1.csv`. That matters beyond tidiness: the landed name is what the downstream reader's `file_pattern` glob matches, so a leftover suffix silently matches nothing rather than failing.

```json
{ "source_zip_handling": { "enabled": true, "source_zip_path": "/Volumes/flowx/staging/uc_6/raw/", "zip_file_pattern": "EE_*-REQUEST_*[Oo][Ff]*.csv.gz.gpg", "target_volume_path": "/Volumes/flowx/staging/uc_6/_extracted/ea_request/", "member_format": "gzip", "pre_extraction_decryption": { "type": "pgp_symmetric", "passphrase_secret": { "secret_catalog": "flowx", "secret_schema": "config", "secret_key": "pgpkey" } }, "delete_source_after_extract": { "action": "delete_now" } } }
```

### Deleting the source archive after extraction (`delete_source_after_extract`)

`source_zip_handling.delete_source_after_extract` controls what happens to the **raw** archive once it has been successfully extracted — distinct from `landing_retention_policy` (§2), which governs the *already-extracted* files. Both the legacy boolean and a new nested object are accepted:

| Spec value | Resolves to | Behavior |
|---|---|---|
| omitted / `null` | `{"action": "delete_now"}` | Today's default, unchanged. |
| `true` | `{"action": "delete_now"}` | Delete this run's own just-extracted archive immediately. |
| `false` | *(internal `"never"`)* | Keep the archive forever. |
| `{"action": "delete_now"}` | — | Identical to `true`. |
| `{"action": "delete_after_x_days", "days": <int >= 0>}` | — | Keep this run's own archive; sweep the landing directory for aged-out archives instead. |

```json
{ "source_zip_handling": { "enabled": true, "source_zip_path": "/Volumes/poc/sales/landing_zip/incoming/", "zip_file_pattern": "orders_*.zip", "target_volume_path": "/Volumes/poc/sales/landing_zip/extracted/orders/", "delete_source_after_extract": { "action": "delete_now" } } }
```
```json
{ "source_zip_handling": { "enabled": true, "source_zip_path": "/Volumes/poc/sales/landing_zip/incoming/", "zip_file_pattern": "orders_*.zip", "target_volume_path": "/Volumes/poc/sales/landing_zip/extracted/orders/", "delete_source_after_extract": { "action": "delete_after_x_days", "days": 30 } } }
```

> [!IMPORTANT]
> **`delete_after_x_days` does NOT delete this run's own just-extracted archive.** A Lakeflow pipeline update can't sleep or schedule future work, so the policy is enforced lazily: once this update's extraction loop has finished, the framework sweeps `source_zip_path` for **every** file matching `zip_file_pattern` whose filesystem modification time is older than `days` days — excluding only archives that **failed to extract in this same run** (a failed archive is the operator's only remaining copy and must survive to be inspected/retried; a *successfully*-extracted archive is already safely unpacked into `target_volume_path`, so it is left to age normally and is swept by whichever run crosses the threshold).
>
> Practical consequences worth internalizing before you rely on this:
> - An archive dropped today is deleted by whichever **future** pipeline update happens to run at least `days` days later — including, with `days: 0`, this same update's own post-extraction sweep.
> - **If no later update ever runs, the archive is never deleted.** This is intentional, documented behavior, not a bug to file.
> - The sweep runs even on an update that matched nothing new to extract, so archives left behind by earlier updates still get cleaned up once they age out.
> - The sweep is scoped to the same `zip_file_pattern` as extraction, so sibling flows sharing one incoming directory never sweep each other's archives.

---

## 6. JSON Explode, Auto-Flatten & JSON-String Column Parsing

Nested JSON documents — structs and arrays, whether native to a JSON source or parsed out of a string column on any other format — are de-nested via `source_config.explode_columns`, `auto_flatten_all`, and (new in v1.3.0) `json_string_columns`, all implemented in `ingestion/json_flattening.py`.

### `explode_columns`: absent vs. present-but-empty

> [!IMPORTANT]
> This distinction is **load-bearing**: it is what prevents a silent cartesian row explosion on an unconfigured source. Getting it backwards changes production row counts, not just column shapes.

| `source_config` | Result |
|---|---|
| `explode_columns` key **absent** | Schema-preserving pass-through — nothing is exploded or flattened. |
| `"explode_columns": null` | Same as absent (this is how a scaffolded template spells "not configured"). |
| `"explode_columns": []` (present **and** empty) | **Auto-flattens every nested struct and explodes every array**, anywhere in the schema, recursively — equivalent to `"auto_flatten_all": true`. |
| `"explode_columns": ["line_items"]` (populated) | Scopes struct-flatten/array-explode treatment to exactly the named top-level column(s), regardless of `auto_flatten_all`. An `array<struct<...>>` column is exploded, then its resulting struct element is flattened in the same step. |
| `"auto_flatten_all": true` | Same effect as present-but-empty `explode_columns`, regardless of whether `explode_columns` itself is present. |

```json
{ "source_config": { "path": "/Volumes/poc/iot/landing_json/", "format": "json", "explode_columns": [] } }
```

**Why the distinction exists, in one sentence:** `explode_columns` is optional, so a spec that simply never mentions it must never undergo *any* implicit flattening — this exact protection exists because an earlier regression let an unconfigured source silently explode into a cartesian product of rows. Explicitly writing `"explode_columns": []`, by contrast, is an unambiguous, deliberate instruction to flatten everything, and only an explicit, literal empty list is treated that way — `null` and "key not present" are not.

A column named in a populated `explode_columns` list that isn't on the DataFrame, or that resolves to neither a struct nor an array, raises `FrameworkConfigError` at runtime. (The onboarding validator only checks that the field is a list of strings — it has no access to the source's actual runtime schema at onboarding time.)

### JSON held in a STRING column: `json_string_columns`

Parquet, CSV, Delta, and Zerobus sources frequently carry a JSON document inside an ordinary string column rather than as a native nested type. `source_config.json_string_columns` parses each named column with `from_json` into a struct **immediately before** the `explode_columns`/auto-flatten pass above runs, so the same struct-flatten/array-explode logic then applies to it unchanged — there is no separate, parallel flattening path for non-JSON source formats. (Struct/array columns Parquet already carries natively need nothing new here — they flow straight into the `explode_columns` handling above.)

Two per-item shapes are accepted:

```json
{
  "source_config": {
    "path": "/Volumes/poc/sales/landing_parquet/",
    "format": "parquet",
    "json_string_columns": [
      { "column": "payload", "schema_ddl": "struct<order_id:string,total:double>" },
      { "column": "attributes" }
    ],
    "explode_columns": []
  }
}
```

- **`{"column": "payload", "schema_ddl": "struct<order_id:string,total:double>"}` — the recommended form.** A fully deterministic `from_json` with an explicit schema; behaves identically on batch and streaming DataFrames.
- **`{"column": "attributes"}`** (or the bare-string shorthand `"attributes"`) — no explicit schema. Falls back to Databricks' *inferring* `from_json`, which persists the inferred schema under a `schemaLocationKey` in the flow's own checkpoint.

> [!WARNING]
> **The no-`schema_ddl` shorthand only works on a streaming DataFrame.** On a batch/materialized source it raises `FrameworkConfigError`:
> ```
> json_string_columns entry '<column>' has no schema_ddl and this flow's DataFrame is not streaming -- schema inference via from_json's schemaLocationKey requires a streaming source with a checkpoint. Supply an explicit schema_ddl (e.g. 'struct<a:string,b:int>') for this column.
> ```
> This is a hard architectural constraint, not a missing feature: the ingestion staged view is built lazily inside a `@dlt.view` closure, and inferring a schema by sampling a value (`schema_of_json(lit(...))`) would require an eager action — illegal on a streaming plan, and a full extra scan on a batch one. Always supply `schema_ddl` for a batch/materialized source.

A `json_string_columns` entry naming a column that doesn't exist on the DataFrame, or that isn't string-typed, also raises `FrameworkConfigError` at runtime. Listing the same column twice is rejected at onboarding time.

---

## 7. Full-Row Streaming Deduplication (`remove_dups`)

`source_config.remove_dups` (boolean, default `false`) applies full-row deduplication to the ingested stream, implemented in `ingestion/dedup.py`.

```json
{
  "source_config": {
    "path": "/Volumes/poc/sales/landing/orders/",
    "format": "csv",
    "remove_dups": true,
    "dedup_watermark": { "event_time_column": "order_ts", "delay_threshold": "2 hours" }
  }
}
```

**Dedup subset.** When `remove_dups: true`, the framework deduplicates over **every column on the DataFrame at that point in the chain, except**:
- any column prefixed `__framework_` (per-run/per-file technical metadata — `__framework_source_file_name`, `__framework_ingestion_timestamp_utc`, and the like — including these would make every row unique and defeat full-row dedup entirely), and
- `_rescued_data`, `_metadata`, `_object_metadata`, `_asn1_decode_error`.

**Position in the ingestion chain: after `explode_columns`/auto-flatten, before `data_standardization_sql`.** This ordering is a correctness requirement, not a style choice: after an array explode, a source row that arrived twice has already become 2×M rows, and only a *post*-explode dedup collapses that correctly. Running dedup before standardization also means standardization expressions are evaluated once per surviving row rather than once per duplicate.

**Which physical row survives a duplicate group is arbitrary** — Spark gives no ordering guarantee here, so the surviving row's `__framework_source_file_name` / `__framework_ingestion_timestamp_utc` are an arbitrary pick among the duplicates. A flow that needs a deterministic "keep the earliest/latest" rule wants an SCD strategy or a windowed `transformation_sql`, not `remove_dups`.

> [!WARNING]
> **Unbounded streaming state.** On a streaming source with no `dedup_watermark` configured, `dropDuplicates` keeps state for **every distinct row seen since the stream started, forever.** On a long-running streaming table this state grows without bound and will eventually degrade or fail the pipeline. The framework logs this once per flow whenever it detects the combination:
> ```
> remove_dups is enabled on a STREAMING DataFrame with no dedup_watermark -- dropDuplicates will keep UNBOUNDED state (every distinct row seen since the stream started is retained forever). Configure source_config.dedup_watermark for any continuously-running stream.
> ```
> Configure `dedup_watermark` for any stream expected to run continuously.

`dedup_watermark` (object, optional — only meaningful alongside `remove_dups: true`):

| Field | Type | Required | Notes |
|---|---|---|---|
| `event_time_column` | string | yes | A `TIMESTAMP` column on the ingested DataFrame. |
| `delay_threshold` | string | yes | A Spark interval string, e.g. `"2 hours"`, `"30 minutes"`. |

When configured **and** the source is streaming, the chain becomes `df.withWatermark(event_time_column, delay_threshold).dropDuplicatesWithinWatermark(subset)` — trading exactness (a late-arriving duplicate outside the watermark survives as a distinct row) for bounded memory. `dropDuplicates` (no watermark) is the **default**, deliberately, so that exactness/memory trade-off is always an explicit operator choice, never one the framework makes silently on your behalf.

On a **non-streaming (batch)** source, `dropDuplicates` is a plain shuffle with no state to bound, and `dedup_watermark` — if present — is simply ignored (logged at `INFO`).

Configuring `dedup_watermark` without `remove_dups: true` is rejected at onboarding time — the block would configure nothing on its own:
```
<path>.dedup_watermark: only meaningful alongside remove_dups: true, but remove_dups is not enabled for this source
```

---

## 8. Technical Metadata & Column Normalization

### Automatic Technical Metadata Injection
When `capture_technical_metadata: true` is set, FlowX automatically enriches Bronze records with standard audit columns:
- `__framework_source_file_name`: Name and path of the ingested file.
- `__framework_source_file_size`: Size in bytes.
- `__framework_source_file_modification_time`: File timestamp on cloud storage.
- `__framework_ingestion_timestamp_utc`: Exact UTC timestamp when the record entered the Delta table.

### Column Normalization

Bronze/raw column-name normalization is configured in exactly one place:

```json
{ "source_config": { "column_normalization": { "enabled": true, "case": "preserve" } } }
```

* **`enabled`** (boolean, default `false`) — the switch. Absent object, absent key and explicit
  `false` all mean off.
* **`case`** (`"lower"` | `"preserve"` | `"upper"`, default `"lower"`) — the case fold, and the
  only configurable part of the transformation.

> [!WARNING]
> **BREAKING (v1.4.0): `source_config.normalize_column_names` is removed.**
> The legacy boolean was a second way to say what `column_normalization.enabled` already says.
> Carrying both meant the framework owned a three-level precedence ladder, a present-vs-truthy
> distinction on `enabled`, and a contradiction warning — roughly forty lines whose only job was
> deciding which of two synonyms won.
>
> Onboarding now **rejects** the key rather than ignoring it. That is deliberate: an ignored
> `normalize_column_names: true` would silently switch renaming *off*, and the first symptom
> would be raw names with spaces and special characters reaching the Delta target — either
> failing the write or rewriting every column name of an existing table.
>
> **Migration** is mechanical:
>
> | Before | After |
> |---|---|
> | `{"normalize_column_names": true}` | `{"column_normalization": {"enabled": true}}` |
> | `{"normalize_column_names": false}` | delete the key |
> | `{"normalize_column_names": true, "column_normalization": {"case": "preserve"}}` | `{"column_normalization": {"enabled": true, "case": "preserve"}}` |
> | `{"column_normalization": {"enabled": true, ...}}` | **no change** — already correct |
>
> Note the third row. Before v1.4.0, an object supplying only `case` deferred enablement to the
> legacy boolean, so `{"case": "preserve"}` on its own was *enabled*. It now reads as written:
> off. If your spec relied on that deferral, add the explicit `"enabled": true`.

Enabling normalization enforces two guarantees — one always-on, one configurable:

1. **Character normalization is always applied, and is case-independent.** Trim leading/trailing
   whitespace → replace every character outside `[A-Za-z0-9_]` with `_` → collapse repeated `_` →
   strip leading/trailing `_` → fall back to a literal `"_"` if nothing is left. Only the
   **case-fold step** is configurable via `case` (default `"lower"`, matching pre-v1.3.0 behaviour
   byte-for-byte). `"preserve"` leaves the source's own letter casing exactly as written;
   `"upper"` upper-cases it.
2. **Collision detection always runs on the lowercased projection of the produced names,
   regardless of `case`.** Unity Catalog and Spark resolve column references case-insensitively by
   default, so two output names differing only by case (e.g. `"Order_ID"` vs. `"order_id"`)
   describe an unusable table, not a valid one. Folding for the duplicate check only — never for
   the casing actually written to the target — is what makes `case: "preserve"`/`"upper"` safe:
   whatever casing lands in the table, the framework has already proven its lowercase projection
   is unique. A collision raises:
   ```
   column_normalization: normalization produced duplicate column names -- '<col_a>' and '<col_b>' both normalize to '<name>'. Rename one of the source columns, or use schema_config_path for explicit renaming instead.
   ```

**Ordering.** Normalization runs immediately after the raw source read and after
`schema_config_path`'s renames — so `explode_columns`, `data_standardization_sql`,
`json_string_columns[].column`, `dq_config.rules[].expression` and every `target_config` column
reference are all written against the **normalized** names, never the source's raw ones.

### Explicit Schema Configuration (`schema_config_path`)
When integrating with rigid upstream enterprise schemas, point `schema_config_path` to an external JSON/YAML dictionary defining target data types, nullability constraints, Unity Catalog comments, and renames:

```yaml
columns:
  - source_name: "tx_id"
    data_type: "string"
    nullable: false
    comment: "Unique transaction identifier"
  - source_name: "amt"
    target_name: "transaction_amount"
    data_type: "decimal(18,4)"
    nullable: true
```
> [!TIP]
> If `schema_config_path` points to a directory (e.g. `/Volumes/catalog/schemas/orders/`), FlowX automatically resolves and loads the most recently modified file in that directory.
