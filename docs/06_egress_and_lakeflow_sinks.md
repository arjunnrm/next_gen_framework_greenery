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
Compresses processed records into a ZIP archive, encrypts the archive with an external partner's PGP public key, and writes the resulting `.zip.pgp` file to a target Volume.
```json
{
  "target_type": "external_sink",
  "target_config": {
    "sink_config": {
      "format": "pgp_zip",
      "destination_volume_path": "/Volumes/{{catalog}}/egress/partner_secure_drop",
      "archive_name_prefix": "daily_settlement_",
      "compression_level": 9,
      "pgp_public_key_secret": {
        "secret_catalog": "poc",
        "secret_schema": "security",
        "secret_key": "partner_public_pgp_key"
      }
    }
  }
}
```

---

## 3. Eager Secret Resolution Principle

To ensure zero downtime and fail-fast validation:
- All secrets referenced in `sink_config` (such as Kafka credentials or PGP public keys) are resolved **eagerly at graph-definition time** during Phase 1.
- If a secret scope or key does not exist, the pipeline fails immediately before starting any compute clusters or writing data.
