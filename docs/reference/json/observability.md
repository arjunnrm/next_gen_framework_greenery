<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# Observability

One entry per `observability[]` element — where telemetry is exported.


!!! info "18 attributes"
    Every attribute below is also available in the Spec Builder's attribute
    inspector — click the **i** beside any field to see this same content
    without leaving the form.


## Summary

| Attribute | Type | Required | Default |
|---|---|---|---|
| [`@observability`](#observability) | array<object> | no | — |
| [`@observability[].auth.credentials`](#observabilityauthcredentials) | object<string,string> | no | — |
| [`@observability[].auth.type`](#observabilityauthtype) | string (enum) | no | — |
| [`@observability[].destination_config.compression`](#observabilitydestination-configcompression) | string (enum) | no | — |
| [`@observability[].destination_config.endpoint`](#observabilitydestination-configendpoint) | string | **yes** | — |
| [`@observability[].destination_config.event_log_tables`](#observabilitydestination-configevent-log-tables) | array<string> | **yes** | — |
| [`@observability[].destination_config.file_format`](#observabilitydestination-configfile-format) | string (enum) | no | — |
| [`@observability[].destination_config.protocol`](#observabilitydestination-configprotocol) | string (enum) | no | — |
| [`@observability[].destination_config.resource_attributes`](#observabilitydestination-configresource-attributes) | object<string,string> | no | — |
| [`@observability[].destination_config.volume_path`](#observabilitydestination-configvolume-path) | string | **yes** | — |
| [`@observability[].enabled`](#observabilityenabled) | boolean | no | — |
| [`@observability[].id`](#observabilityid) | string | **yes** | — |
| [`@observability[].mode`](#observabilitymode) | string (enum) | no | — |
| [`@observability[].retry.backoff_multiplier`](#observabilityretrybackoff-multiplier) | integer | no | — |
| [`@observability[].retry.max_attempts`](#observabilityretrymax-attempts) | integer | no | — |
| [`@observability[].timeout_ms`](#observabilitytimeout-ms) | integer | no | — |
| [`@observability[].type`](#observabilitytype) | string (enum) | **yes** | — |
| [`@observability_enabled`](#observability-enabled) | boolean | no | — |

## Attributes

### `@observability` { #observability }

Sets observability[].


**Type** `array<object>` · **Required** no · **Section** Destinations


```json
{
  "observability": [
    {
      "...": "one object per entry"
    }
  ]
}
```


!!! tip "Best practice"

    - Each entry becomes one object in a JSON array. A wholly blank entry is dropped on save.


---

### `@observability[].auth.credentials` { #observabilityauthcredentials }

Credential map.


Credential map. Values must be env:<VAR> or secret:<scope>:<key> references — a literal secret is rejected.


**Type** `object<string,string>` · **Required** no · **Section** Destinations


```json
{
  "auth": {
    "credentials": {
      "option_name": "value"
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Keys are written verbatim — a typo becomes a silently ignored option, not an error.


---

### `@observability[].auth.type` { #observabilityauthtype }

Authentication scheme.


Authentication scheme. Network destinations only — Volume exports authenticate through Unity Catalog.


**Type** `string (enum)` · **Required** no · **Section** Destinations


```json
{
  "auth": {
    "type": "BEARER_TOKEN"
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Allowed values: BEARER_TOKEN, API_KEY, BASIC_AUTH, NONE.


---

### `@observability[].destination_config.compression` { #observabilitydestination-configcompression }

Compression applied to exported telemetry files.


Compression applied to exported telemetry files. The choices offered depend on destination_config.file_format — SNAPPY is only valid for PARQUET.


**Type** `string (enum)` · **Required** no · **Section** Destinations


```json
{
  "destination_config": {
    "compression": "GZIP"
  }
}
```


!!! tip "Best practice"

    - narrowed by file_format
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Allowed values: GZIP, SNAPPY, NONE.


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html)


---

### `@observability[].destination_config.endpoint` { #observabilitydestination-configendpoint }

OTLP collector URL.


OTLP collector URL. Volume exports need no endpoint — only a Volume path.


**Type** `string` · **Required** yes · **Section** Destinations


```json
{
  "destination_config": {
    "endpoint": "https://otel-collector.internal.net:4318/v1/logs"
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html)


---

### `@observability[].destination_config.event_log_tables` { #observabilitydestination-configevent-log-tables }

Fully-qualified catalog.schema.event_log_table names streamed together in continuous mode.


**Type** `array<string>` · **Required** yes · **Section** Destinations


```json
{
  "destination_config": {
    "event_log_tables": [
      "{{catalog}}.silver_example.event_log"
    ]
  }
}
```


!!! tip "Best practice"

    - continuous mode only
    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Entered as a comma-separated list; written to the spec as a JSON array of strings.


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html)


---

### `@observability[].destination_config.file_format` { #observabilitydestination-configfile-format }

On-disk format of exported telemetry files.


On-disk format of exported telemetry files. JSONL is one JSON object per line — the usual choice for telemetry. Selecting a format narrows destination_config.compression to the codecs that format supports.


**Type** `string (enum)` · **Required** no · **Section** Destinations


```json
{
  "destination_config": {
    "file_format": "JSONL"
  }
}
```


!!! tip "Best practice"

    - drives the compression choices
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Allowed values: JSONL, JSON, PARQUET.


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html)


---

### `@observability[].destination_config.protocol` { #observabilitydestination-configprotocol }

Wire protocol used to talk to the collector.


**Type** `string (enum)` · **Required** no · **Section** Destinations


```json
{
  "destination_config": {
    "protocol": "OTLP_HTTP_JSON"
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Allowed values: OTLP_HTTP_JSON, OTLP_HTTP_PROTOBUF, OTLP_GRPC.


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html)


---

### `@observability[].destination_config.resource_attributes` { #observabilitydestination-configresource-attributes }

OpenTelemetry resource attributes attached to every exported record, e.g.


OpenTelemetry resource attributes attached to every exported record, e.g. service.name, deployment.environment.


**Type** `object<string,string>` · **Required** no · **Section** Destinations


```json
{
  "destination_config": {
    "resource_attributes": {
      "option_name": "value"
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Keys are written verbatim — a typo becomes a silently ignored option, not an error.


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html)


---

### `@observability[].destination_config.volume_path` { #observabilitydestination-configvolume-path }

Unity Catalog Volume directory telemetry files are written to.


**Type** `string` · **Required** yes · **Section** Destinations


```json
{
  "destination_config": {
    "volume_path": "/Volumes/{{catalog}}/observability/app_logs/"
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html)


---

### `@observability[].enabled` { #observabilityenabled }

Master enable switch for this destination.


Master enable switch for this destination. A disabled destination is dropped entirely by the config loader — callers never see it.


**Type** `boolean` · **Required** no · **Section** Destinations


```json
{
  "enabled": true
}
```


!!! tip "Best practice"

    - Omitting the attribute is not the same as setting it false — check the default above.


---

### `@observability[].id` { #observabilityid }

Unique destination identifier.


**Type** `string` · **Required** yes · **Section** Destinations


```json
{
  "id": "dest-otlp-example"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


---

### `@observability[].mode` { #observabilitymode }

triggered extracts the event log once after a pipeline run, bounded by task_id and a start/end time window.


triggered extracts the event log once after a pipeline run, bounded by task_id and a start/end time window. continuous streams every configured event-log table together as an always-on process.


**Type** `string (enum)` · **Required** no · **Section** Destinations


```json
{
  "mode": "triggered"
}
```


!!! tip "Best practice"

    - Allowed values: triggered, continuous.


---

### `@observability[].retry.backoff_multiplier` { #observabilityretrybackoff-multiplier }

Exponential backoff multiplier between retries.


Exponential backoff multiplier between retries. Must be greater than 1. OTLP_CONSUMER only.


**Type** `integer` · **Required** no · **Section** Destinations


```json
{
  "retry": {
    "backoff_multiplier": 30
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


---

### `@observability[].retry.max_attempts` { #observabilityretrymax-attempts }

How many times a failed network export is retried.


How many times a failed network export is retried. Minimum 1. OTLP_CONSUMER only.


**Type** `integer` · **Required** no · **Section** Destinations


```json
{
  "retry": {
    "max_attempts": 3
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


---

### `@observability[].timeout_ms` { #observabilitytimeout-ms }

Request timeout in milliseconds.


Request timeout in milliseconds. Not applicable to Volume exports.


**Type** `integer` · **Required** no · **Section** Destinations


```json
{
  "timeout_ms": 5000
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


---

### `@observability[].type` { #observabilitytype }

Where telemetry is exported.


**Type** `string (enum)` · **Required** yes · **Section** Destinations


```json
{
  "type": "DATABRICKS_VOLUME"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Allowed values: DATABRICKS_VOLUME, OTLP_CONSUMER.


---

### `@observability_enabled` { #observability-enabled }

Master switch.


Master switch. When off, the observability array is omitted from the spec entirely and no telemetry is exported.


**Type** `boolean` · **Required** no · **Section** Observability


```json
{
  "observability_enabled": true
}
```


!!! tip "Best practice"

    - turn on to configure destinations
    - Omitting the attribute is not the same as setting it false — check the default above.


---
