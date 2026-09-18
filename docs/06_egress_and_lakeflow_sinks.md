# 📤 Metaflow — Egress & Lakeflow Sinks

> **Audience**: Integration engineers and data architects building external data syndication pipelines to cloud object stores, Kafka clusters, or external partner Volumes.

---

## 1. Lakeflow Native In-Graph Sink Architecture

In Metaflow, egress sinks are **not** separate post-deployment batch jobs or standalone Spark notebooks. Instead, they are registered natively inside the Lakeflow pipeline DAG using:
- `dlt.create_sink(...)`: Defines the external destination target and format.
- `@dlt.append_flow(...)`: Streams data from an upstream view or table directly into the sink with transactional checkpointing.

```
┌─────────────────────────────────────────────────────────────┐
│                 UPSTREAM DLT TABLE / VIEW                   │
│               poc.gold.customer_analytics                   │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼ @dlt.append_flow
┌─────────────────────────────────────────────────────────────┐
│                 LAKEFLOW SINK WRITER                        │
│ • Format: Delta / Kafka / PGP+ZIP                           │
│ • Eager Secret Resolution at Graph Definition Time          │
│ • Transactional Micro-Batch Checkpointing                   │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                  EXTERNAL DESTINATION                       │
│ • External Volume Path (/Volumes/partner/export/...)        │
│ • Kafka Topic (events.orders.stream)                        │
│ • PGP-Encrypted ZIP Archive File                            │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. Sink Format Specifications

Metaflow supports 3 production sink formats configured via `target_config.sink_config.format`:

### 2.1 Delta Format (`format: "delta"`)
Directly exports Delta Lake tables to an external Volume or object storage location.
```json
{
  "target_type": "sink",
  "target_config": {
    "sink_config": {
      "format": "delta",
      "path": "/Volumes/{{catalog}}/egress/gold_orders_export",
      "mode": "append",
      "options": {
        "checkpointLocation": "/Volumes/{{catalog}}/egress/checkpoints/orders"
      }
    }
  }
}
```

### 2.2 Kafka Egress Format (`format: "kafka"`)
Streams processed records into a real-time Kafka topic with SASL/SSL authentication.
```json
{
  "target_type": "external_sink",
  "target_config": {
    "sink_config": {
      "format": "kafka",
      "topic": "events.telecom.cdr",
      "kafka_options": {
        "kafka.bootstrap.servers": "kafka.prod.internal:9092",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": "secret:kafka_creds:jaas_config"
      }
    }
  }
}
```

### 2.3 PGP-Encrypted ZIP Export (`format: "pgp_zip"`)
This framework's own custom Lakeflow sink (`archive/pgp_zip_sink.py::PgpZipDataSource`, a real `pyspark.sql.datasource.DataSource`). Each micro-batch is a two-step **stage-then-archive**: `write()` (executor side) stages one raw-row file per non-empty partition into `sink_config.path` (the staging directory), then `commit()` (driver side) zips exactly that micro-batch's staged files — optionally AES-password-protecting the ZIP and/or PGP-encrypting+signing it — into one finished archive under `post_export_archive.output_zip_path`.
```json
{
  "target_type": "external_sink",
  "target_config": {
    "sink_config": {
      "format": "pgp_zip",
      "path": "/Volumes/{{catalog}}/egress/partner_staging",
      "staged_file_format": "json",
      "post_export_archive": {
        "enabled": true,
        "output_zip_path": "/Volumes/{{catalog}}/egress/partner_secure_drop",
        "export_file_name_format": "{timestamp}_{batch_id}_export.zip",
        "pgp_encryption": {
          "enabled": true,
          "recipient_public_key_secret": {
            "secret_catalog": "poc",
            "secret_schema": "security",
            "secret_key": "partner_public_pgp_key"
          }
        }
      }
    }
  }
}
```

#### Staged file format inside the archive (`staged_file_format`, v1.6.0)

`sink_config.staged_file_format` controls the format of the staged per-partition files that end up
inside the exported archive. It is meaningful for `pgp_zip` **only** — the attribute is rejected on
presence for `"delta"`/`"kafka"`, which are native Lakeflow sinks with no framework staging step
(`onboarding/spec_validator.py::_validate_sink_config`).

| Value | Staged file shape |
|---|---|
| `"json"` (default when absent) | JSON-Lines — one JSON object per row, the only pre-v1.6.0 behaviour. Non-JSON-native values (dates, decimals, binary) are stringified rather than failing the micro-batch. |
| `"csv"` | RFC-4180 CSV **with a header row**, one staged file per non-empty partition per micro-batch (each file carries its own header). Header/column order follows the sink's declared write schema when Spark supplies one (stable across partitions and micro-batches), falling back to the first row's own field order. `None` serialises as an **empty cell**; a nested struct/array serialises as its **JSON text** (never a Python repr), so a downstream consumer can still parse it. |

Both formats share the same tolerance philosophy: the sink's job is to archive the data for
downstream consumption, never to crash a micro-batch over one awkward column type.

#### CSV dialect (`staged_file_options`, v1.7.4)

Before v1.7.4 the staged CSV was written by `csv.DictWriter` with **no dialect arguments at all**,
i.e. Python's `excel` default: comma-separated, always headered, CRLF-terminated. A supplier
interface specifying anything else could not be expressed, and there was no workaround short of
post-processing the finished archive.

`sink_config.staged_file_options` is valid **only** alongside `staged_file_format: "csv"` —
JSON-Lines has no delimiter and no header row, so accepting these there would let a spec assert a
file shape nothing produces.

| Key | Type | Default | Notes |
|---|---|---|---|
| `delimiter` | string | `","` | Exactly **one** character — Python's csv writer cannot emit a multi-character delimiter. |
| `include_header` | boolean | `true` | `false` emits data rows only. |
| `line_terminator` | string | `"crlf"` | `"crlf"` (RFC 4180, and the pre-v1.7.4 behaviour) or `"lf"`. Spelled as a **name** because a JSON string cannot carry a bare control character unambiguously, and `"
"` vs `"\r\n"` is a classic silent-escaping trap. |

#### Archive container (`post_export_archive.archive_format`, v1.7.4)

| Value | Output |
|---|---|
| `"zip"` (default) | One AES-capable ZIP per micro-batch, holding one file per non-empty partition. Unchanged; the encrypted variant keeps its `.zip.pgp` suffix. |
| `"gzip"` | The micro-batch's staged files are **concatenated into one gzip stream** (a gzip holds exactly one member) and named `<export_file_name_format>.<csv\|jsonl>.gz`, or `....gz.gpg` when encrypted. |

Concatenation is only safe because every staged file in one micro-batch shares a schema. Since a
headered CSV writes its header **per staged file** (the writer cannot know which partition lands
first), all but the first header are dropped on concatenation — which is why `include_header` and
`archive_format` interact. JSON-Lines has no header and never has a line dropped.

`post_export_archive.secret` (an AES password on the ZIP) has no meaning for `"gzip"`.

#### Symmetric egress encryption (`pgp_encryption.passphrase_secret`, v1.7.4)

`pgp_encryption` now accepts **exactly one** of:

- `recipient_public_key_secret` — encrypt to a recipient's public key (asymmetric, unchanged), optionally signed via `sign_with_private_key_secret`;
- `passphrase_secret` — encrypt under a **shared passphrase** (symmetric), the shape `gpg --symmetric --cipher-algo AES256` produces.

Setting both is rejected: a PGP message is one or the other, never both. Signing is **not**
available for symmetric encryption — it requires a sender keypair — and naming a signing key
alongside `passphrase_secret` is rejected rather than ignored. Before v1.7.4 the recipient key was
unconditionally required, so symmetric egress was unreachable from a spec.

```json
{ "sink_config": { "format": "pgp_zip", "path": "/Volumes/flowx/staging/uc_6/output/_staging/tel/", "staged_file_format": "csv", "staged_file_options": { "delimiter": "|", "include_header": true, "line_terminator": "lf" }, "post_export_archive": { "enabled": true, "output_zip_path": "/Volumes/flowx/staging/uc_6/output/", "export_file_name_format": "EE_2026-08-20-TELEPHONE_1of1", "archive_format": "gzip", "pgp_encryption": { "enabled": true, "passphrase_secret": { "secret_catalog": "flowx", "secret_schema": "config", "secret_key": "pgpkey" } } } } }
```

→ emits `EE_2026-08-20-TELEPHONE_1of1.csv.gz.gpg`, decryptable with a stock `gpg --decrypt`.

---

#### Export trigger (`export_trigger`, v1.7.5)

`sink_config.export_trigger` decides **what drives the export**:

| Value | Behaviour |
|---|---|
| `"per_micro_batch"` (default when absent) | The sink is fed from this flow's own staged view. One archive per micro-batch of an append-only stream. The only pre-v1.7.5 behaviour. |
| `"per_update"` | An update-scoped **pulse** drives the sink; the rows are read as a **batch**. Exactly one archive per pipeline update — including an update that ingested no new rows. |

##### The problem `"per_update"` exists to solve

Two facts, each individually reasonable, combined into a dead end:

1. A Lakeflow sink is **streaming-only** — *"Only streaming queries are supported. Batch queries are not supported."*
2. Delta **refuses to stream from a fully-recomputed table** (`DELTA_SOURCE_TABLE_IGNORE_CHANGES`). `skipChangeCommits` is refused framework-wide because it silently drops changed rows.

So an **aggregating** target — a `materialized_view`, or any `TRUNCATE_AND_LOAD` flow — could be computed and published, and then had **no way to leave the platform as a file**. Not a missing feature: a structural contradiction. A `GROUP BY` result was simply not exportable.

##### How it works: separate the trigger from the payload

The default uses one stream for both — the staged view's rows are simultaneously *what schedules the write* and *what gets written*. That is why a non-streamable payload takes the trigger down with it. `"per_update"` splits them:

- **Trigger** — a one-row streaming *pulse* (`_flowx_export_pulse` — **one** temporary dataset per pipeline, shared by every `per_update` sink; it carries no data, so N sinks reading it is N edges, not N streams), deliberately independent of every business feed. It carries no data. Its only job is to make the append flow genuinely streaming, satisfying Lakeflow's constraint **honestly** rather than by relabelling metadata.
- **Payload** — the staged view, read as a batch `dlt.read`. An aggregation is legal precisely because nothing streams it.

The two are joined on a constant literal so the pulse's single row fans out across the payload; the gate column is dropped before any row reaches the sink.

```json
"sink_config": {
  "format": "pgp_zip",
  "path": "/Volumes/{{catalog}}/staging/uc_6/output/_staging/tel/",
  "staged_file_format": "csv",
  "staged_file_options": { "delimiter": "|", "include_header": true, "line_terminator": "lf" },
  "export_trigger": "per_update",
  "post_export_archive": {
    "enabled": true,
    "output_zip_path": "/Volumes/{{catalog}}/staging/uc_6/output/",
    "export_file_name_format": "EE_${export_file_date}-LEIDOS_TELEPHONE_${export_file_sequence}",
    "archive_format": "gzip"
  }
}
```

##### Why the pulse is `rate-micro-batch`, and not a business feed or a plain `rate` stream

This is the design decision worth understanding, because the obvious alternative is wrong.

An earlier design pulsed off an upstream business stream. That fires per **micro-batch of that stream**, not per **update** — so an update in which the upstream advanced no offsets would recompute the aggregate and write **no file at all**. Silent missing output on a contractual feed is a worse failure than the error this feature removes.

A clock-independent pulse is **update-scoped** instead — but the choice of source matters, and the obvious one is wrong. Spark's plain `rate` source counts rows as **wall-clock seconds since its checkpoint was created**, so under a triggered update's `AvailableNow` it is a race. A live side-by-side probe, both variants feeding a counting handler:

| Pulse source | Fresh checkpoint | Incremental update |
|---|---|---|
| `rate` | 1 row | **0 rows** |
| `rate-micro-batch` (`rowsPerBatch=1`) | 1 row | 1 row |

The `rate` variant produced **zero rows on an incremental update** — the export would have been silently skipped, the exact failure this trigger exists to prevent (and exactly what happened on UC6's first green pipeline run: gold correct, four empty sinks). `rate-micro-batch` emits exactly `rowsPerBatch` rows per micro-batch regardless of the clock, and `AvailableNow` runs it as a single micro-batch: **one row, every update, deterministically.** No `.limit()` is applied — `rowsPerBatch=1` already yields one row, and a streaming `LIMIT` is a risk of its own.

##### Notes

- The payload is read with `dlt.read`, never `spark.read.table` — lineage stays inside the graph (Single-Read DAG mandate rule 2), so the dependency is visible in the Lakeflow DAG and Lakeflow still orders the payload's computation before the export.
- Quarantined rows are filtered and **every `__framework_*` column is dropped** before rows reach the sink, on all three sink paths (`per_micro_batch`, `per_update`, and the `external_sink` export). A sink is an external interface with a contractual layout; before v1.7.5 `__framework_ingestion_timestamp_utc`, `__framework_pipeline_run_id` and `__framework_record_id` leaked into every `pgp_zip` file (found live: UC6's first exports had a 5-column header where the spec says `targetAreaID|telephone`). Lineage stays on the governed tables the sink reads; a consumer that wants a run id in a file projects it in `transformation_sql` under a business name.
- `pgp_zip` only. `delta` and `kafka` are native Lakeflow sinks whose write cadence Lakeflow itself owns, so the attribute is presence-rejected there — the same contract as `staged_file_format`.
- **Changing the pulse source on an existing pipeline needs a one-time full refresh of the pulse tables.** A streaming checkpoint records its source's offset format; swapping the source (as v1.7.5 did, `rate` → `rate-micro-batch`) makes the next incremental update fail with `STREAM_FAILED ... No usable value for offset` (json4s cannot read the old offset as a long) — and DLT auto-retries into the same wall. Changing which table a sink flow streams *from* (v1.7.5 replaced the per-sink pulses with one shared `_flowx_export_pulse`) breaks the sink flows' own checkpoints with `DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE`; a sink flow is not a selectable table, so recover with a whole-pipeline `start_update(full_refresh=True)` (a one-time migration; 264s live). A brand-new pipeline is unaffected.
- **Do not** work around the streaming error by relabelling an aggregating flow as a `streaming_table`. That silences a plan-time error and converts it into a runtime one, which is strictly worse.

---

## 3. Eager Secret Resolution Principle

To ensure zero downtime and fail-fast validation:
- All secrets referenced in `sink_config` (such as Kafka credentials or PGP public keys) are resolved **eagerly at graph-definition time** during Phase 1.
- If a secret scope or key does not exist, the pipeline fails immediately before starting any compute clusters or writing data.
