# 📤 FlowX — Egress & Lakeflow Sinks

> **Audience**: Integration engineers and data architects building external data syndication pipelines to cloud object stores, Kafka clusters, or external partner Volumes.

---

## 1. Lakeflow Native In-Graph Sink Architecture

In FlowX, egress sinks are **not** separate post-deployment batch jobs or standalone Spark notebooks. Instead, they are registered natively inside the Lakeflow pipeline DAG using:
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

FlowX supports 3 production sink formats configured via `target_config.sink_config.format`:

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

## 3. Eager Secret Resolution Principle

To ensure zero downtime and fail-fast validation:
- All secrets referenced in `sink_config` (such as Kafka credentials or PGP public keys) are resolved **eagerly at graph-definition time** during Phase 1.
- If a secret scope or key does not exist, the pipeline fails immediately before starting any compute clusters or writing data.
