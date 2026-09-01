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

---

## 3. Eager Secret Resolution Principle

To ensure zero downtime and fail-fast validation:
- All secrets referenced in `sink_config` (such as Kafka credentials or PGP public keys) are resolved **eagerly at graph-definition time** during Phase 1.
- If a secret scope or key does not exist, the pipeline fails immediately before starting any compute clusters or writing data.
