<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# Observability

One entry per `observability[]` element — where telemetry is exported.


!!! info "18 attributes · 73 FAQs"
    Every attribute below is also available in the Spec Builder's attribute
    inspector — click the **i** beside any field to see this same content
    without leaving the form. Badges: <span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Flow kind</span> <span class="fx-badge fx-ver">vX.Y+ added in</span> <span class="fx-badge fx-only">Spec Builder section</span>.


**Recipes and deep dive:** [Pillar 4 · Observability](../../pillars/observability.md) · [Schema tree](tree.md) · [Removed & rejected](removed.md)


## Summary

| Attribute | Type | Required | Default | Since |
|---|---|---|---|---|
| [`@observability`](#observability) | array<object> | no | — | — |
| [`@observability[].auth.credentials`](#observabilityauthcredentials) | object<string,string> | no | — | — |
| [`@observability[].auth.type`](#observabilityauthtype) | string (enum) | no | — | — |
| [`@observability[].destination_config.compression`](#observabilitydestination-configcompression) | string (enum) | no | — | — |
| [`@observability[].destination_config.endpoint`](#observabilitydestination-configendpoint) | string | **yes** | — | — |
| [`@observability[].destination_config.event_log_tables`](#observabilitydestination-configevent-log-tables) | array<string> | **yes** | — | — |
| [`@observability[].destination_config.file_format`](#observabilitydestination-configfile-format) | string (enum) | no | — | — |
| [`@observability[].destination_config.protocol`](#observabilitydestination-configprotocol) | string (enum) | no | — | — |
| [`@observability[].destination_config.resource_attributes`](#observabilitydestination-configresource-attributes) | object<string,string> | no | — | — |
| [`@observability[].destination_config.volume_path`](#observabilitydestination-configvolume-path) | string | **yes** | — | — |
| [`@observability[].enabled`](#observabilityenabled) | boolean | no | `true` | — |
| [`@observability[].id`](#observabilityid) | string | **yes** | — | — |
| [`@observability[].mode`](#observabilitymode) | string (enum) | no | `"triggered"` | — |
| [`@observability[].retry.backoff_multiplier`](#observabilityretrybackoff-multiplier) | integer | no | `2.0` | — |
| [`@observability[].retry.max_attempts`](#observabilityretrymax-attempts) | integer | no | `3` | — |
| [`@observability[].timeout_ms`](#observabilitytimeout-ms) | integer | no | — | — |
| [`@observability[].type`](#observabilitytype) | string (enum) | **yes** | — | — |
| [`@observability_enabled`](#observability-enabled) | boolean | no | — | — |

## Attributes

### `@observability` { #observability }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Observability</span> <span class="fx-badge fx-only">Destinations</span>

Sets observability[].


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<object>` | — | — | — |

**Persisted in** `config.observability_config.(one row per destination)`


=== "JSON"

    ```json
    {
      "observability": [
        {
          "...": "one object per entry"
        }
      ]
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- @observability is persisted as its own column
    SELECT destination_id,
           (one row per destination),
           is_active, updated_at
    FROM   <catalog>.config.observability_config
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Each entry becomes one object in a JSON array. A wholly blank entry is dropped on save.

**FAQs** (4)

??? question "If omitted · What happens if I omit the observability array entirely?"

    No telemetry destinations are onboarded and `_validate_observability_destinations` returns an empty list without error. Unlike ingestion/transformation/reconciliation arrays, `observability` deliberately does not count toward the "at least one flow array must be non-empty" rule — it is optional and inert when absent.

??? question "Format gotcha · Can I write a blank or partially-filled entry in the observability array?"

    In the Spec Builder UI a wholly blank entry is dropped on save, so it never reaches the JSON spec. A partially-filled entry that does reach the spec (e.g. missing `id` or `type`) fails validation as required fields are missing.

??? question "Performance impact · Does adding entries to observability slow down the pipeline update?"

    Negligible for `triggered` mode — export runs as a separate downstream job task after the pipeline update completes, not inside it. `continuous` mode runs as its own standing pipeline, so it adds no load to the source pipelines either; see docs/08 §1 and §6.

??? question "Edge case · If two destinations reference the same event_log table, is it read twice?"

    No. `resolve_event_log_tables` de-duplicates the table union across all continuous destinations, so a table named by more than one destination is still read only once into one `@dlt.view`, per docs/08 §2.5.


**See also:** [Pillar 4 · Observability](../../pillars/observability.md) · [Attribute dictionary](../../00_master_reference_index.md#9-observability-config-schema) · [Schema tree](tree.md#tree-root-observability) · [Spec Builder · Observability tab](../../console/spec_builder.md#spec-root-and-observability) · [Observability dashboard](../../console/observability_dashboard.md) · [Control dashboard · Observability & Audit](../../console/control_dashboard.md#observability-audit) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `@observability[].auth.credentials` { #observabilityauthcredentials }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Observability</span> <span class="fx-badge fx-only">Destinations</span>

Credential map.


Credential map. Values must be env:<VAR> or secret:<scope>:<key> references — a literal secret is rejected.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `object<string,string>` | — | — | — |

**Persisted in** `config.observability_config.auth_config_json`


=== "JSON"

    ```json
    {
      "auth": {
        "credentials": {
          "option_name": "value"
        }
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- @observability[].auth.credentials lives inside the auth_config_json JSON document; inspect it with from_json / get_json_object
    SELECT destination_id,
           auth_config_json,
           is_active, updated_at
    FROM   <catalog>.config.observability_config
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Keys are written verbatim — a typo becomes a silently ignored option, not an error.

**FAQs** (5)

??? question "If omitted · What happens if auth.credentials is left out but auth.type is set to BEARER_TOKEN?"

    `_validate_observability_auth` requires `credentials` as a dict when `auth_type` is `BEARER_TOKEN`, `API_KEY`, or `BASIC_AUTH` — omitting it fails validation via `check_dict(credentials, credentials_path, errors, required=True)`. For `auth.type: NONE` (or no `auth` object at all), `credentials` is not needed.

??? question "Format gotcha · Can I put a plain password or token string directly in auth.credentials?"

    No — every credential value must match `env:<VAR_NAME>` or `secret:<scope>:<key>`; `check_credential_ref` rejects a literal secret value at onboarding time. Resolution happens at dispatch: `env:` reads an environment variable, `secret:` calls `dbutils.secrets.get(scope, key)`. See docs/08 §2.3.

??? question "Performance impact · Does resolving credentials at every export add noticeable overhead?"

    Negligible — `resolve_credential` is a single env-var read or one `dbutils.secrets.get` call per dispatch, not a per-row operation, so it does not scale with telemetry volume.

??? question "Edge case · What if I add an extra key to auth.credentials that a given auth.type does not expect, like a stray field?"

    Keys are written verbatim into the config; an unexpected key is not validated against a strict allowlist inside `credentials` and is silently unused rather than rejected. Only the specific keys each `auth.type` needs (e.g. `token` for `BEARER_TOKEN`) are read by `destination_dispatcher.py::build_auth_headers`.

??? question "Edge case · Does auth.credentials apply to a DATABRICKS_VOLUME destination?"

    No — `auth` is ignored entirely for `DATABRICKS_VOLUME`; Volume exports authenticate through native Unity Catalog permissions, not through this block. See docs/08 §2.1.


**See also:** [Pillar 4 · Observability](../../pillars/observability.md) · [Attribute dictionary](../../00_master_reference_index.md#9-observability-config-schema) · [Schema tree](tree.md#tree-observability-authcredentials) · [Spec Builder · Observability tab](../../console/spec_builder.md#spec-root-and-observability) · [Observability dashboard](../../console/observability_dashboard.md) · [Control dashboard · Observability & Audit](../../console/control_dashboard.md#observability-audit) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `@observability[].auth.type` { #observabilityauthtype }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Observability</span> <span class="fx-badge fx-only">Destinations</span>

Authentication scheme.


Authentication scheme. Network destinations only — Volume exports authenticate through Unity Catalog.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `BEARER_TOKEN`, `API_KEY`, `BASIC_AUTH`, `NONE` | — |

**Persisted in** `config.observability_config.auth_config_json`


=== "JSON"

    ```json
    {
      "auth": {
        "type": "BEARER_TOKEN"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- @observability[].auth.type lives inside the auth_config_json JSON document; inspect it with from_json / get_json_object
    SELECT destination_id,
           auth_config_json,
           is_active, updated_at
    FROM   <catalog>.config.observability_config
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Allowed values: BEARER_TOKEN, API_KEY, BASIC_AUTH, NONE.

**FAQs** (4)

??? question "If omitted · What is used for auth if I omit the whole auth object?"

    `build_auth_headers` treats an absent/`None` `auth_config` the same as `type: NONE`: `auth_type = (auth_config or {}).get("type", "NONE")`, so no Authorization header is sent. This is fine for a collector that needs no authentication.

??? question "Format gotcha · Are the auth.type enum values case-sensitive?"

    Yes — only the exact uppercase strings `BEARER_TOKEN`, `API_KEY`, `BASIC_AUTH`, `NONE` are accepted by `check_string(..., allowed_values=ALLOWED_OBSERVABILITY_AUTH_TYPES)`; any other casing or value fails onboarding validation.

??? question "Performance impact · Does choosing BASIC_AUTH over BEARER_TOKEN cost more per export?"

    Negligible — `BASIC_AUTH` adds one extra `base64.b64encode` call over a `BEARER_TOKEN` header build; both happen once per dispatch attempt, not per record.

??? question "Edge case · Does auth.type matter for a DATABRICKS_VOLUME destination?"

    No — the whole `auth` block, including `type`, is ignored for `DATABRICKS_VOLUME` destinations, which rely on Unity Catalog permissions instead. It is only meaningful for `OTLP_CONSUMER`. See docs/08 §2.1.


**See also:** [Pillar 4 · Observability](../../pillars/observability.md) · [Attribute dictionary](../../00_master_reference_index.md#9-observability-config-schema) · [Schema tree](tree.md#tree-observability-authtype) · [Spec Builder · Observability tab](../../console/spec_builder.md#spec-root-and-observability) · [Observability dashboard](../../console/observability_dashboard.md) · [Control dashboard · Observability & Audit](../../console/control_dashboard.md#observability-audit) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `@observability[].destination_config.compression` { #observabilitydestination-configcompression }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Observability</span> <span class="fx-badge fx-only">Destinations</span>

Compression applied to exported telemetry files.


Compression applied to exported telemetry files. The choices offered depend on destination_config.file_format — SNAPPY is only valid for PARQUET.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | — | — |

**Persisted in** `config.observability_config.destination_config_json`


=== "JSON"

    ```json
    {
      "destination_config": {
        "compression": "GZIP"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- @observability[].destination_config.compression lives inside the destination_config_json JSON document; inspect it with from_json / get_json_object
    SELECT destination_id,
           destination_config_json,
           is_active, updated_at
    FROM   <catalog>.config.observability_config
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - narrowed by file_format
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Allowed values: GZIP, SNAPPY, NONE.

**FAQs** (4)

??? question "If omitted · What compression is used if destination_config.compression is not set?"

    It defaults to `"none"` per the JSON schema's `observabilityCompression` default, meaning telemetry files are written uncompressed.

??? question "Format gotcha · Does compression accept the value 'NONE' in uppercase?"

    No — the allowed set is exactly `{"GZIP", "gzip", "none", ""}`; uppercase `"NONE"` is rejected by `check_string(..., allowed_values=ALLOWED_OBSERVABILITY_COMPRESSION)`. Only `"GZIP"`/`"gzip"` enable gzip, and only lowercase `"none"` or an empty string disable it.

??? question "Performance impact · Is there a real cost saving from turning on GZIP compression for exported telemetry?"

    Yes for storage and network transfer of exported files/payloads — gzip typically shrinks JSON/JSONL text significantly — but it is not validated against `file_format`/`type` beyond the enum check itself, so weigh it against CPU spent compressing on every export.

??? question "Edge case · Does the schema restrict compression choices to what a given file_format supports, e.g. is SNAPPY valid for PARQUET here?"

    No — this framework's `observabilityCompression` enum is only `GZIP`/`gzip`/`none`/`""` for both `DATABRICKS_VOLUME` and `OTLP_CONSUMER`; there is no `SNAPPY` or `PARQUET`-specific compression option, and `file_format` itself only allows `JSONL`/`JSON`. Any claim of PARQUET/SNAPPY support does not match `spec_validator.py` or `onboarding_spec.schema.json`.


**See also:** [Pillar 4 · Observability](../../pillars/observability.md) · [Attribute dictionary](../../00_master_reference_index.md#9-observability-config-schema) · [Schema tree](tree.md#tree-observability-observabilitydestination-configcompression) · [Spec Builder · Observability tab](../../console/spec_builder.md#spec-root-and-observability) · [Observability dashboard](../../console/observability_dashboard.md) · [Control dashboard · Observability & Audit](../../console/control_dashboard.md#observability-audit) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html)


---

### `@observability[].destination_config.endpoint` { #observabilitydestination-configendpoint }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Observability</span> <span class="fx-badge fx-only">Destinations</span>

OTLP collector URL.


OTLP collector URL. Volume exports need no endpoint — only a Volume path.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.observability_config.destination_config_json`


=== "JSON"

    ```json
    {
      "destination_config": {
        "endpoint": "https://otel-collector.internal.net:4318/v1/logs"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- @observability[].destination_config.endpoint lives inside the destination_config_json JSON document; inspect it with from_json / get_json_object
    SELECT destination_id,
           destination_config_json,
           is_active, updated_at
    FROM   <catalog>.config.observability_config
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What happens if I set type to OTLP_CONSUMER but leave out destination_config.endpoint?"

    Onboarding rejects the flow: `check_string(endpoint, ..., required=True)` inside `_validate_observability_destination_config` fails because `endpoint` is required whenever `destination_type == "OTLP_CONSUMER"`.

??? question "Format gotcha · What URL formats are accepted for endpoint?"

    It must start with `http://` or `https://`; anything else fails with `'{path}.endpoint: must be a full http:// or https:// URL, got ...'`. The schema additionally requires `format: uri`.

??? question "Performance impact · Does the endpoint value affect export latency?"

    Only insofar as network distance/collector load do — the framework itself adds no overhead based on the URL's shape. Use `timeout_ms` and `retry` to bound how long a slow endpoint is allowed to hold up the export.

??? question "Edge case · Do I need destination_config.endpoint for a DATABRICKS_VOLUME destination?"

    No — Volume exports need only `volume_path`; `endpoint` is meaningful, and required, only for `OTLP_CONSUMER`. See docs/08 §2.2.


**See also:** [Pillar 4 · Observability](../../pillars/observability.md) · [Attribute dictionary](../../00_master_reference_index.md#9-observability-config-schema) · [Schema tree](tree.md#tree-observability-observabilitydestination-configendpoint) · [Spec Builder · Observability tab](../../console/spec_builder.md#spec-root-and-observability) · [Observability dashboard](../../console/observability_dashboard.md) · [Control dashboard · Observability & Audit](../../console/control_dashboard.md#observability-audit) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html)


---

### `@observability[].destination_config.event_log_tables` { #observabilitydestination-configevent-log-tables }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Observability</span> <span class="fx-badge fx-only">Destinations</span>

Fully-qualified catalog.schema.event_log_table names streamed together in continuous mode.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<string>` | — | — | — |

**Persisted in** `config.observability_config.destination_config_json` · **Behaviour changed in** v1.4.0


=== "JSON"

    ```json
    {
      "destination_config": {
        "event_log_tables": [
          "{{catalog}}.silver_example.event_log"
        ]
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- @observability[].destination_config.event_log_tables lives inside the destination_config_json JSON document; inspect it with from_json / get_json_object
    SELECT destination_id,
           destination_config_json,
           is_active, updated_at
    FROM   <catalog>.config.observability_config
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - continuous mode only
    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Entered as a comma-separated list; written to the spec as a JSON array of strings.

**FAQs** (4)

??? question "If omitted · What happens if mode is continuous but destination_config.event_log_tables is omitted?"

    Onboarding rejects it: `'{path}.destination_config.event_log_tables: is required when mode is 'continuous' -- the continuous observability pipeline has no upstream task to resolve a pipeline from...'`. Without a real spec row, the streaming pipeline falls back at runtime to the pipeline resource's own `dataflow.otel_streaming.event_log_tables` config, or fails with 'there is nothing to stream' if that too is empty (docs/08 §6.1).

??? question "Format gotcha · What format must each entry in event_log_tables use?"

    Each entry must be a fully-qualified, exactly three-part `catalog.schema.table` string (`parts = entry.split("."); len(parts) != 3`). A two-part or unqualified name is rejected per-entry by `_validate_observability_event_log_tables`.

??? question "Performance impact · Does listing many event_log_tables slow down the continuous pipeline?"

    Each table is a real streaming read that is unioned into `unified_event_log`; more tables means more concurrent streaming sources for that one continuous pipeline. `resolve_event_log_tables` at least de-duplicates the same table named by multiple destinations so it is read only once, per docs/08 §2.5.

??? question "Edge case · What happens if I set event_log_tables on a triggered destination?"

    It is rejected: `'only meaningful when mode is 'continuous', but this destination's mode is 'triggered' (the triggered engine resolves its pipeline from the upstream task run)'`. The triggered engine has no use for a fixed table list since it resolves its pipeline dynamically from the run context.


**See also:** [Pillar 4 · Observability](../../pillars/observability.md) · [Attribute dictionary](../../00_master_reference_index.md#9-observability-config-schema) · [Schema tree](tree.md#tree-observability-observabilitydestination-configevent-log-tables) · [Spec Builder · Observability tab](../../console/spec_builder.md#spec-root-and-observability) · [Observability dashboard](../../console/observability_dashboard.md) · [Control dashboard · Observability & Audit](../../console/control_dashboard.md#observability-audit) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html)


---

### `@observability[].destination_config.file_format` { #observabilitydestination-configfile-format }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Observability</span> <span class="fx-badge fx-only">Destinations</span>

On-disk format of exported telemetry files.


On-disk format of exported telemetry files. JSONL is one JSON object per line — the usual choice for telemetry. Selecting a format narrows destination_config.compression to the codecs that format supports.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | — | — |

**Persisted in** `config.observability_config.destination_config_json`


=== "JSON"

    ```json
    {
      "destination_config": {
        "file_format": "JSONL"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- @observability[].destination_config.file_format lives inside the destination_config_json JSON document; inspect it with from_json / get_json_object
    SELECT destination_id,
           destination_config_json,
           is_active, updated_at
    FROM   <catalog>.config.observability_config
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - drives the compression choices
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Allowed values: JSONL, JSON, PARQUET.

**FAQs** (4)

??? question "If omitted · What file format is used for Volume exports if file_format is not set?"

    It defaults to `"JSONL"` per the schema's default and docs/08 §2.2 — one JSON object per line, the usual choice for telemetry.

??? question "Format gotcha · What values does file_format actually accept?"

    Only `"JSONL"` or `"JSON"` — `check_string(config.get("file_format"), ..., allowed_values={"JSONL", "JSON"})` for `DATABRICKS_VOLUME`. There is no `PARQUET` option in this schema despite it appearing plausible; do not use it.

??? question "Performance impact · Is JSONL more efficient than JSON for large telemetry exports?"

    JSONL (one object per line) is generally cheaper to append to and stream-process incrementally than a single JSON array document, which is why docs/08 calls it "the usual choice for telemetry" — but the framework does not measure or enforce this; it is an operational recommendation, not a validated constraint.

??? question "Edge case · Does file_format apply to an OTLP_CONSUMER destination?"

    No — `file_format` is read only inside the `DATABRICKS_VOLUME` branch of `_validate_observability_destination_config`; an `OTLP_CONSUMER` destination has no `file_format` field in its schema branch at all (payloads are OTel `ResourceLogs`-shaped, not files).


**See also:** [Pillar 4 · Observability](../../pillars/observability.md) · [Attribute dictionary](../../00_master_reference_index.md#9-observability-config-schema) · [Schema tree](tree.md#tree-observability-observabilitydestination-configfile-format) · [Spec Builder · Observability tab](../../console/spec_builder.md#spec-root-and-observability) · [Observability dashboard](../../console/observability_dashboard.md) · [Control dashboard · Observability & Audit](../../console/control_dashboard.md#observability-audit) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html)


---

### `@observability[].destination_config.protocol` { #observabilitydestination-configprotocol }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Observability</span> <span class="fx-badge fx-only">Destinations</span>

Wire protocol used to talk to the collector.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | — | — |

**Persisted in** `config.observability_config.destination_config_json`


=== "JSON"

    ```json
    {
      "destination_config": {
        "protocol": "OTLP_HTTP_JSON"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- @observability[].destination_config.protocol lives inside the destination_config_json JSON document; inspect it with from_json / get_json_object
    SELECT destination_id,
           destination_config_json,
           is_active, updated_at
    FROM   <catalog>.config.observability_config
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Allowed values: OTLP_HTTP_JSON, OTLP_HTTP_PROTOBUF, OTLP_GRPC.

**FAQs** (4)

??? question "If omitted · What protocol is used if I omit destination_config.protocol on an OTLP_CONSUMER destination?"

    The schema default is `"OTLP_HTTP_JSON"`. The validator itself does not force a default when the key is absent (it only checks the value when present), but the schema and runtime treat missing as `OTLP_HTTP_JSON`.

??? question "Format gotcha · What exact values does protocol accept, and is it OTLP_HTTP_PROTOBUF or OTLP_HTTP_PROTO?"

    The allowed values are exactly `OTLP_HTTP_JSON`, `OTLP_HTTP_PROTO`, `OTLP_GRPC` per both `spec_validator.py` and the JSON schema — it is `OTLP_HTTP_PROTO`, not `OTLP_HTTP_PROTOBUF`. Using the wrong spelling fails onboarding validation.

??? question "Performance impact · Does choosing OTLP_GRPC instead of OTLP_HTTP_JSON change export throughput?"

    Not validated at onboarding — the schema comment notes only `OTLP_HTTP_JSON` is actually implemented by `destination_dispatcher.py` today, so any performance difference for `OTLP_HTTP_PROTO`/`OTLP_GRPC` is unverified; treat those as accepted-but-unimplemented and confirm at runtime.

??? question "Edge case · What happens at dispatch time if I set protocol to OTLP_GRPC?"

    It passes onboarding validation (the value is in the allowed enum) but the schema's own `$comment` states only `OTLP_HTTP_JSON` is implemented by `destination_dispatcher.py` — verify actual dispatch behaviour at runtime since the validator does not check implementation coverage. See docs/08 Extending section.


**See also:** [Pillar 4 · Observability](../../pillars/observability.md) · [Attribute dictionary](../../00_master_reference_index.md#9-observability-config-schema) · [Schema tree](tree.md#tree-observability-observabilitydestination-configprotocol) · [Spec Builder · Observability tab](../../console/spec_builder.md#spec-root-and-observability) · [Observability dashboard](../../console/observability_dashboard.md) · [Control dashboard · Observability & Audit](../../console/control_dashboard.md#observability-audit) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html)


---

### `@observability[].destination_config.resource_attributes` { #observabilitydestination-configresource-attributes }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Observability</span> <span class="fx-badge fx-only">Destinations</span>

OpenTelemetry resource attributes attached to every exported record, e.g.


OpenTelemetry resource attributes attached to every exported record, e.g. service.name, deployment.environment.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `object<string,string>` | — | — | — |

**Persisted in** `config.observability_config.destination_config_json`


=== "JSON"

    ```json
    {
      "destination_config": {
        "resource_attributes": {
          "option_name": "value"
        }
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- @observability[].destination_config.resource_attributes lives inside the destination_config_json JSON document; inspect it with from_json / get_json_object
    SELECT destination_id,
           destination_config_json,
           is_active, updated_at
    FROM   <catalog>.config.observability_config
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Keys are written verbatim — a typo becomes a silently ignored option, not an error.

**FAQs** (4)

??? question "If omitted · What happens if resource_attributes is left out?"

    It defaults to an effectively empty map (`{}` per docs/08 §2.2); only the framework's own built-in resource attributes are attached to exported records, with nothing merged in or overridden.

??? question "Format gotcha · What value types can resource_attributes hold?"

    It must be an object whose keys and values are both strings — `check_dict_of_str` enforces this; a non-string value (e.g. a number or nested object) fails validation.

??? question "Performance impact · Does adding many resource_attributes slow down each export?"

    Negligible — they are merged once per `ResourceLogs` entry by `destination_dispatcher.py::merge_resource_attributes` immediately before dispatch, a cheap dict merge, not a per-record loop.

??? question "Edge case · What happens if a key in resource_attributes has a typo or duplicates a framework-built attribute?"

    A typo'd key is written verbatim and simply becomes an extra, unused attribute — it is not validated against any known set. A key matching a framework-built attribute name overrides it, since `merge_resource_attributes` merges the spec's values in, taking precedence over same-named keys the framework already built.


**See also:** [Pillar 4 · Observability](../../pillars/observability.md) · [Attribute dictionary](../../00_master_reference_index.md#9-observability-config-schema) · [Schema tree](tree.md#tree-observability-observabilitydestination-configresource-attributes) · [Spec Builder · Observability tab](../../console/spec_builder.md#spec-root-and-observability) · [Observability dashboard](../../console/observability_dashboard.md) · [Control dashboard · Observability & Audit](../../console/control_dashboard.md#observability-audit) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html)


---

### `@observability[].destination_config.volume_path` { #observabilitydestination-configvolume-path }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Observability</span> <span class="fx-badge fx-only">Destinations</span>

Unity Catalog Volume directory telemetry files are written to.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.observability_config.destination_config_json`


=== "JSON"

    ```json
    {
      "destination_config": {
        "volume_path": "/Volumes/{{catalog}}/observability/app_logs/"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- @observability[].destination_config.volume_path lives inside the destination_config_json JSON document; inspect it with from_json / get_json_object
    SELECT destination_id,
           destination_config_json,
           is_active, updated_at
    FROM   <catalog>.config.observability_config
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What happens if type is DATABRICKS_VOLUME and volume_path is omitted?"

    Onboarding rejects the flow via `check_string(volume_path, ..., required=True)`. At runtime, if a misconfiguration somehow reaches the continuous engine, docs/08 §6.2 notes a continuous Volume destination missing `volume_path` is instead skipped with a WARNING rather than failing the whole pipeline update.

??? question "Format gotcha · What prefix must volume_path use, and can I use a ${param} placeholder in it?"

    It must start with `/Volumes/` — `'{path}.volume_path: must start with \'/Volumes/\', got ...'` otherwise. Unlike other path fields, `transformation/parameters.py` deliberately excludes `destination_config.volume_path` from `${param}` substitution, since a `${param}`-prefixed value would newly fail the strict `/Volumes/` check — write the literal path instead.

??? question "Performance impact · Does the choice of Volume path affect export performance?"

    Negligible from the framework's side — it is a plain Unity Catalog Volume write; any performance difference would come from the underlying storage/volume characteristics, not from anything this attribute controls.

??? question "Edge case · Is volume_path required for an OTLP_CONSUMER destination too?"

    No — `volume_path` is only checked inside the `DATABRICKS_VOLUME` branch of `_validate_observability_destination_config`; an `OTLP_CONSUMER` destination uses `endpoint` instead and needs no Volume path at all.


**See also:** [Pillar 4 · Observability](../../pillars/observability.md) · [Attribute dictionary](../../00_master_reference_index.md#9-observability-config-schema) · [Schema tree](tree.md#tree-observability-observabilitydestination-configvolume-path) · [Spec Builder · Observability tab](../../console/spec_builder.md#spec-root-and-observability) · [Observability dashboard](../../console/observability_dashboard.md) · [Control dashboard · Observability & Audit](../../console/control_dashboard.md#observability-audit) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html)


---

### `@observability[].enabled` { #observabilityenabled }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Observability</span> <span class="fx-badge fx-only">Destinations</span>

Master enable switch for this destination.


Master enable switch for this destination. A disabled destination is dropped entirely by the config loader — callers never see it.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `boolean` | `true` | — | — |

**Persisted in** `config.observability_config.enabled`


=== "JSON"

    ```json
    {
      "enabled": true
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- @observability[].enabled is persisted as its own column
    SELECT destination_id,
           enabled,
           is_active, updated_at
    FROM   <catalog>.config.observability_config
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Omitting the attribute is not the same as setting it false — check the default above.

**FAQs** (4)

??? question "If omitted · If I don't set observability[].enabled, is the destination active or not?"

    It defaults to `true` — the schema's default is `true` and omitting the attribute is not the same as setting it `false`. A destination is active unless explicitly disabled.

??? question "Format gotcha · Does enabled accept anything besides a plain boolean, like the string 'true'?"

    No — `check_bool` requires an actual JSON boolean; a string like `"true"` fails validation. Use `true`/`false` literals.

??? question "Performance impact · Is there any cost to leaving a destination in the spec with enabled: false?"

    Negligible — a disabled destination is dropped entirely by `config_loader.py`'s `parse_config_rows` (`if not row.get("enabled", False): continue`), so callers never see it and no export runs for it; it only occupies a row in `observability_config`.

??? question "Edge case · How is a disabled destination reported — as an error, a skip, or silently?"

    Silently, by design — the doc explicitly notes there is no "enabled" flag callers must remember to check, because `config_loader.py` filters it out before any consumer sees it; it is not logged as an error or a skip event.


**See also:** [Pillar 4 · Observability](../../pillars/observability.md) · [Attribute dictionary](../../00_master_reference_index.md#9-observability-config-schema) · [Schema tree](tree.md#tree-observability-enabled) · [Spec Builder · Observability tab](../../console/spec_builder.md#spec-root-and-observability) · [Observability dashboard](../../console/observability_dashboard.md) · [Control dashboard · Observability & Audit](../../console/control_dashboard.md#observability-audit) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `@observability[].id` { #observabilityid }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Observability</span> <span class="fx-badge fx-only">Destinations</span>

Unique destination identifier.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.observability_config.destination_id`


=== "JSON"

    ```json
    {
      "id": "dest-otlp-example"
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- @observability[].id is persisted as its own column
    SELECT destination_id,
           destination_id,
           is_active, updated_at
    FROM   <catalog>.config.observability_config
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What happens if I leave out observability[].id?"

    Onboarding rejects the destination: `check_string(destination_id, f"{label}.id", errors, required=True)` fails since `id` has no default and is always required.

??? question "Format gotcha · Are there naming rules for observability[].id beyond being non-empty?"

    The schema only requires a string with `minLength: 1`; there is no pattern constraint, but the id becomes `destination_id` in `observability_config`, so a stable, descriptive name like `dest-otlp-example` is recommended.

??? question "Performance impact · Does the id value have any runtime performance implication?"

    None directly — it is purely a lookup key. Its only operational effect is on how re-onboarding behaves (see edge_case).

??? question "Edge case · What happens if two destinations in the same spec share the same id?"

    Onboarding rejects it: `'{label}.id: '<id>' is already used by observability[<n>] -- destination id must be unique within a spec'`. Re-onboarding the same `id` for the same dataflow group instead upserts (MERGEs) the same row rather than duplicating it, per docs/08 §2.1.


**See also:** [Pillar 4 · Observability](../../pillars/observability.md) · [Attribute dictionary](../../00_master_reference_index.md#9-observability-config-schema) · [Schema tree](tree.md#tree-observability-id) · [Spec Builder · Observability tab](../../console/spec_builder.md#spec-root-and-observability) · [Observability dashboard](../../console/observability_dashboard.md) · [Control dashboard · Observability & Audit](../../console/control_dashboard.md#observability-audit) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `@observability[].mode` { #observabilitymode }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Observability</span> <span class="fx-badge fx-only">Destinations</span>

triggered extracts the event log once after a pipeline run, bounded by task_id and a start/end time window.


triggered extracts the event log once after a pipeline run, bounded by task_id and a start/end time window. continuous streams every configured event-log table together as an always-on process.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | `"triggered"` | `triggered`, `continuous` | — |

**Persisted in** `config.observability_config.mode`


=== "JSON"

    ```json
    {
      "mode": "triggered"
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- @observability[].mode is persisted as its own column
    SELECT destination_id,
           mode,
           is_active, updated_at
    FROM   <catalog>.config.observability_config
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Allowed values: triggered, continuous.

**FAQs** (4)

??? question "If omitted · What mode does a destination run in if mode is not specified?"

    It resolves to `"triggered"` everywhere — both the schema default and `config_loader.py`'s defensive `row.get("mode") or DEFAULT_DESTINATION_MODE` read — so a pre-v1.3.0 `observability_config` row with no `mode` column value keeps working with no migration needed.

??? question "Format gotcha · What are the exact accepted values for mode?"

    Only `"triggered"` or `"continuous"` — `check_string(..., allowed_values=ALLOWED_OBSERVABILITY_MODES)`; any other string fails onboarding validation.

??? question "Performance impact · Is continuous mode more expensive to run than triggered?"

    Yes in kind, not just degree — `triggered` runs once per pipeline update as a bounded job task, while `continuous` is a standing, always-on Lakeflow pipeline streaming event logs continuously, so it consumes compute even when no source pipeline is currently updating. See docs/08 §1.

??? question "Edge case · Can the same destination_id be served by both triggered and continuous mode at once?"

    No — `mode` is exactly what stops one destination being served, and therefore double-exported, by both observability engines; each destination is served by exactly one of the two. See docs/08 §2.1.


**See also:** [Pillar 4 · Observability](../../pillars/observability.md) · [Attribute dictionary](../../00_master_reference_index.md#9-observability-config-schema) · [Schema tree](tree.md#tree-observability-mode) · [Spec Builder · Observability tab](../../console/spec_builder.md#spec-root-and-observability) · [Observability dashboard](../../console/observability_dashboard.md) · [Control dashboard · Observability & Audit](../../console/control_dashboard.md#observability-audit) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `@observability[].retry.backoff_multiplier` { #observabilityretrybackoff-multiplier }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Observability</span> <span class="fx-badge fx-only">Destinations</span>

Exponential backoff multiplier between retries.


Exponential backoff multiplier between retries. Must be greater than 1. OTLP_CONSUMER only.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `integer` | `2.0` | — | — |

**Persisted in** `config.observability_config.retry_config_json`


=== "JSON"

    ```json
    {
      "retry": {
        "backoff_multiplier": 30
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- @observability[].retry.backoff_multiplier lives inside the retry_config_json JSON document; inspect it with from_json / get_json_object
    SELECT destination_id,
           retry_config_json,
           is_active, updated_at
    FROM   <catalog>.config.observability_config
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What backoff_multiplier is used if I don't set it?"

    It defaults to `2.0` per the schema and `_validate_observability_retry`'s implicit default; exponential backoff doubles the delay between retry attempts unless overridden.

??? question "Format gotcha · What values are rejected for backoff_multiplier?"

    It must be a number strictly greater than 1 and not a boolean — the validator explicitly checks `isinstance(backoff_multiplier, bool) or not isinstance(..., (int, float)) or backoff_multiplier <= 1` and errors `'must be a number > 1, got ...'` otherwise. `1.0` or lower is rejected.

??? question "Performance impact · Does a high backoff_multiplier make failed exports take much longer to give up?"

    Yes — combined with `retry.max_attempts`, a larger multiplier grows the delay between attempts exponentially, though docs/08 §2.4 notes the computed delay is capped at 30s per attempt, bounding the worst case.

??? question "Edge case · Does backoff_multiplier apply to a DATABRICKS_VOLUME destination?"

    No — `retry` (including `backoff_multiplier`) is documented as `OTLP_CONSUMER` only in docs/08 §2.1; a Volume export has no network retry loop to back off.


**See also:** [Pillar 4 · Observability](../../pillars/observability.md) · [Attribute dictionary](../../00_master_reference_index.md#9-observability-config-schema) · [Schema tree](tree.md#tree-observability-retrybackoff-multiplier) · [Spec Builder · Observability tab](../../console/spec_builder.md#spec-root-and-observability) · [Observability dashboard](../../console/observability_dashboard.md) · [Control dashboard · Observability & Audit](../../console/control_dashboard.md#observability-audit) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `@observability[].retry.max_attempts` { #observabilityretrymax-attempts }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Observability</span> <span class="fx-badge fx-only">Destinations</span>

How many times a failed network export is retried.


How many times a failed network export is retried. Minimum 1. OTLP_CONSUMER only.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `integer` | `3` | — | min `1`, max `10` |

**Persisted in** `config.observability_config.retry_config_json`


=== "JSON"

    ```json
    {
      "retry": {
        "max_attempts": 3
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- @observability[].retry.max_attempts lives inside the retry_config_json JSON document; inspect it with from_json / get_json_object
    SELECT destination_id,
           retry_config_json,
           is_active, updated_at
    FROM   <catalog>.config.observability_config
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · How many retries happen if max_attempts is not set?"

    It defaults to `3` — both the schema default and `destination_dispatcher.py`'s `DEFAULT_MAX_ATTEMPTS = 3` used via `retry_config.get("max_attempts", DEFAULT_MAX_ATTEMPTS)`.

??? question "Format gotcha · Is there an upper bound on max_attempts, and does the validator enforce it?"

    The JSON schema caps it at `maximum: 10`, but `spec_validator.py`'s `_validate_observability_retry` only checks `check_int(..., minimum=1)` — it does not enforce the upper bound. A value of 1 is the floor; values above 10 are not rejected at onboarding by the Python validator even though the schema disallows them, so validate against both gates.

??? question "Performance impact · Does setting max_attempts high risk long-running or stuck exports?"

    Yes — more attempts combined with exponential `backoff_multiplier` (capped at 30s per attempt per docs/08 §2.4) extends the worst-case time before an export gives up; retried status codes are `429, 500, 502, 503, 504`.

??? question "Edge case · Does max_attempts apply to a DATABRICKS_VOLUME destination?"

    No — like `backoff_multiplier`, `retry.max_attempts` is documented as `OTLP_CONSUMER` only; Volume writes have no network retry loop.


**See also:** [Pillar 4 · Observability](../../pillars/observability.md) · [Attribute dictionary](../../00_master_reference_index.md#9-observability-config-schema) · [Schema tree](tree.md#tree-observability-retrymax-attempts) · [Spec Builder · Observability tab](../../console/spec_builder.md#spec-root-and-observability) · [Observability dashboard](../../console/observability_dashboard.md) · [Control dashboard · Observability & Audit](../../console/control_dashboard.md#observability-audit) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `@observability[].timeout_ms` { #observabilitytimeout-ms }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Observability</span> <span class="fx-badge fx-only">Destinations</span>

Request timeout in milliseconds.


Request timeout in milliseconds. Not applicable to Volume exports.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `integer` | — | — | min `1` |

**Persisted in** `config.observability_config.retry_config_json`


=== "JSON"

    ```json
    {
      "timeout_ms": 5000
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- @observability[].timeout_ms lives inside the retry_config_json JSON document; inspect it with from_json / get_json_object
    SELECT destination_id,
           retry_config_json,
           is_active, updated_at
    FROM   <catalog>.config.observability_config
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What request timeout applies if timeout_ms is omitted?"

    It defaults to `5000` (5 seconds) — `destination_dispatcher.py`'s `DEFAULT_TIMEOUT_MS = 5000`, read via `retry_config.get("timeout_ms", DEFAULT_TIMEOUT_MS)`.

??? question "Format gotcha · Where does timeout_ms sit in the spec — nested under retry or at the destination top level?"

    It sits at the destination's top level, a sibling of `retry`, not nested inside it — but it is folded into `retry_config_json` for storage by `upsert_observability_config`. Get the nesting wrong (e.g. inside `retry`) and it will not be read as `timeout_ms`.

??? question "Performance impact · Does raising timeout_ms help with a slow OTLP collector?"

    Yes, that is its purpose — docs/08's troubleshooting table for `destination_outage` (network timeout / HTTP 503 / ConnectionTimeout) suggests checking collector health and increasing `timeout_ms` / `retry.max_attempts` together.

??? question "Edge case · Does timeout_ms apply to a DATABRICKS_VOLUME destination?"

    No — per docs/08 §2.1, `timeout_ms` is 'Not applicable to Volume exports'; it is only meaningful for `OTLP_CONSUMER` network calls.


**See also:** [Pillar 4 · Observability](../../pillars/observability.md) · [Attribute dictionary](../../00_master_reference_index.md#9-observability-config-schema) · [Schema tree](tree.md#tree-observability-timeout-ms) · [Spec Builder · Observability tab](../../console/spec_builder.md#spec-root-and-observability) · [Observability dashboard](../../console/observability_dashboard.md) · [Control dashboard · Observability & Audit](../../console/control_dashboard.md#observability-audit) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `@observability[].type` { #observabilitytype }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Observability</span> <span class="fx-badge fx-only">Destinations</span>

Where telemetry is exported.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `DATABRICKS_VOLUME`, `OTLP_CONSUMER` | — |

**Persisted in** `config.observability_config.destination_type`


=== "JSON"

    ```json
    {
      "type": "DATABRICKS_VOLUME"
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- @observability[].type is persisted as its own column
    SELECT destination_id,
           destination_type,
           is_active, updated_at
    FROM   <catalog>.config.observability_config
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Allowed values: DATABRICKS_VOLUME, OTLP_CONSUMER.

**FAQs** (4)

??? question "If omitted · What happens if I omit observability[].type?"

    Onboarding rejects the destination: `check_string(destination_type, f"{label}.type", errors, required=True, allowed_values=...)` fails since `type` has no default and is always required.

??? question "Format gotcha · What are the exact accepted values for type?"

    Only `"DATABRICKS_VOLUME"` or `"OTLP_CONSUMER"` — `ALLOWED_OBSERVABILITY_DESTINATION_TYPES = {"DATABRICKS_VOLUME", "OTLP_CONSUMER"}`; any other string fails validation.

??? question "Performance impact · Is one destination type more expensive than the other?"

    Not validated as a performance concern by the framework — `DATABRICKS_VOLUME` writes files to Unity Catalog storage while `OTLP_CONSUMER` makes network calls with retry/timeout overhead; the relevant cost knobs (`retry`, `timeout_ms`) exist only for `OTLP_CONSUMER`.

??? question "Edge case · Does type determine which destination_config fields are required?"

    Yes — `type` drives conditional requirements in both the schema's `allOf`/`if`/`then` blocks and `_validate_observability_destination_config`: `DATABRICKS_VOLUME` requires `volume_path`, `OTLP_CONSUMER` requires `endpoint`. Getting `type` wrong means the wrong set of required fields is enforced.


**See also:** [Pillar 4 · Observability](../../pillars/observability.md) · [Attribute dictionary](../../00_master_reference_index.md#9-observability-config-schema) · [Schema tree](tree.md#tree-observability-type) · [Spec Builder · Observability tab](../../console/spec_builder.md#spec-root-and-observability) · [Observability dashboard](../../console/observability_dashboard.md) · [Control dashboard · Observability & Audit](../../console/control_dashboard.md#observability-audit) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `@observability_enabled` { #observability-enabled }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Observability</span> <span class="fx-badge fx-only">Observability</span>

Master switch.


Master switch. When off, the observability array is omitted from the spec entirely and no telemetry is exported.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `boolean` | — | — | — |

=== "JSON"

    ```json
    {
      "observability_enabled": true
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- @observability_enabled is persisted as its own column
    SELECT destination_id,
           *,
           is_active, updated_at
    FROM   <catalog>.config.observability_config
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - turn on to configure destinations
    - Omitting the attribute is not the same as setting it false — check the default above.

**FAQs** (4)

??? question "If omitted · What happens in the generated spec if observability_enabled is left off in the Spec Builder?"

    This is a Spec Builder UI-only helper, not a real onboarding-spec key (`emit: "never"` in `databricks-app/config/registry/observability.json`) — when off, the `observability` array is omitted from the emitted spec entirely and no telemetry is exported. It never appears in the actual JSON sent to onboarding.

??? question "Format gotcha · Will observability_enabled: true ever appear as a literal key in my onboarding spec JSON?"

    No — it is a boolean toggle local to the Spec Builder (`server/core/serializer.py`/`deserializer.py` read and set it only on the in-memory builder document), used purely to decide whether to render the Destinations section and whether to emit the real `observability` array; `spec_validator.py` has no `ALLOWED_ROOT_KEYS` entry for it.

??? question "Performance impact · Does toggling observability_enabled in the Builder cost anything at pipeline runtime?"

    Negligible — it only controls Builder-side visibility and whether `observability` is emitted; it has no runtime code path of its own since it never reaches the spec or the validator.

??? question "Edge case · What happens if I turn observability_enabled off after already adding destinations in the Builder?"

    Per `Builder.jsx`, the emitted spec only includes `observability` when `observability_enabled === true` (`if(obs.length && r.v["@observability_enabled"]===true) out.observability=obs`); turning it off drops the whole array from the generated spec even if destination entries are still filled in the UI, so re-enabling it before saving is required to keep them.


**See also:** [Pillar 4 · Observability](../../pillars/observability.md) · [Attribute dictionary](../../00_master_reference_index.md#9-observability-config-schema) · [Spec Builder · Observability tab](../../console/spec_builder.md#spec-root-and-observability) · [Observability dashboard](../../console/observability_dashboard.md) · [Control dashboard · Observability & Audit](../../console/control_dashboard.md#observability-audit) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

