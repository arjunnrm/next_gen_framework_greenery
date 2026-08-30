# Onboarding Template Field Reference

> See also: [README.md](README.md) for the full Metaflow documentation set.

Every field below is enforced, field-by-field, by
`src/NextGen_Metadata_Framework/lakeflow_framework/onboarding/spec_validator.py`, and every
field appears at least once, with a real value, in
`onboarding_templates/pipeline_onboarding_template.json` (and its byte-for-byte structural
twin, `pipeline_onboarding_template.yaml` — see
[09_onboarding_yaml_json.md](09_onboarding_yaml_json.md) for format parity). This doc walks
the whole spec shape field by field, deep enough that you should be able to write a
correct onboarding spec from this doc alone, without opening `spec_validator.py` yourself.

It intentionally goes deeper on raw field enumeration — types, required/optional,
allowed values, sample literal strings you can search the template for — than
[01_control_metadata_schema.md](01_control_metadata_schema.md), which groups the same
fields by control-table column and narrative context; read that doc for the "why" behind
the schema's overall shape, [02_cdc_load_strategies.md](02_cdc_load_strategies.md) for
*when* to pick which `cdc_load_strategy`, [07_reconciliation.md](07_reconciliation.md) and
[23_lakeflow_sinks.md](23_lakeflow_sinks.md) for the full mechanics behind reconciliation
and sinks respectively, and [16_encryption_and_secrets.md](16_encryption_and_secrets.md)
for the encryption/secret design. All six docs describe the same live schema and are kept
in sync; where this doc and one of those overlaps, this doc links out rather than
re-deriving the narrative.

> Every field table below cites a **Sample value** copied verbatim from
> `onboarding_templates/pipeline_onboarding_template.json` (or, where the template doesn't
> exercise it, a real `test_specs/*.json` file, named explicitly) so you can search for that
> literal string to see it in context. "Required" always means *as enforced by
> `spec_validator.py`*, not merely "documented as required" — every requirement below is a
> real hard-error path, cited by function name where it isn't obvious.

---

## 1. Top-level spec shape

The file passed to `02_onboarding_engine.py` — JSON or YAML — validated top-to-bottom by
`validate_spec` in `spec_validator.py`:

| Attribute | Type | Required | Description | Sample value |
|---|---|---|---|---|
| `dataflow_group_id` | string | **yes** | Unique id for this pipeline group; every control-table row parsed from this file is upserted under it. | `"dfg_template_example"` |
| `pipeline_parameters` | object (string → any) | no | Runtime parameters injected via `spark.conf` and substituted into `${param}` placeholders inside `transformation_sql`/reconciliation's `filter_condition`/`transform_sql`. Values are rendered as SQL literals — strings single-quoted, everything else via `str()` — see [transformation_sql](#34-transformation_sql-parameters-union-and-the-stream-keyword) below for the exact substitution rule and its one sharp edge. | `{"filter_country": "US", "min_amount": "0"}` |
| `ingestion_flows` | array of objects | see below | Bronze ingestion definitions. §2. | 4 objects in the template |
| `transformation_flows` | array of objects | see below | Silver/Gold transformation definitions. §3. | 11 objects |
| `reconciliation_flows` | array of objects | see below | Cross-dataset comparison/self-healing definitions. §5. | 2 objects |
| `observability` | array of objects | no | Telemetry destinations for the standalone DLT observability engine — unrelated to this spec's own tables. §6. | 2 objects |

**At least one of the three flow arrays must be non-empty** — `validate_spec` raises when
all three are empty/absent, but any combination of the three (including just one) is a
valid spec; a `reconciliation_flows`-only spec (nothing to ingest or transform) is
perfectly legal, for example. `observability` is independent of this requirement — it never
counts toward "non-empty" on its own.

`{{catalog}}`/`{{env}}` anywhere in the file — including nested inside strings such as
Volume paths — are substituted with the `catalog`/`env` job-parameter values *before*
JSON/YAML parsing (`onboarding/spec_loader.py::substitute_environment_placeholders`); every
sample value below that contains `{{catalog}}`/`{{env}}` is copied verbatim from the
template, unsubstituted, exactly as you'd write it in your own spec.

There is no top-level `depends_on_dataflow_group_ids` field — dependency ordering is a
Lakeflow Jobs concern (job task `depends_on`), not the framework's.

---

## 2. `ingestion_flows[]`

### 2.1 Fields common to every ingestion flow

| Attribute | Type | Required | Description | Sample value |
|---|---|---|---|---|
| `dataflow_id` | string | **yes** | Unique id for this flow; referenced by `transformation_flows[].dataflow_id` (dependency-ordering label only — the real data dependency is `source_inputs[].table`) and used to build the internal `_<target_table>_staged` view name. | `"df_template_ingest"` |
| `source_system` | string | no | Free text, unvalidated — never checked by `spec_validator.py`, purely descriptive metadata carried through to the control table. | `"example_source_system"` |
| `source_database` | string | no | Free text, unvalidated. | `"example_landing_db"` |
| `source_table_name` | string | no | Free text, unvalidated. | `"example_raw_table"` |
| `source_description` | string | no | Free text, unvalidated; becomes the target Delta table's `COMMENT`. | `"Template: Bronze ingestion from a Volume (Auto Loader) with regex file selection, quarantine, encryption, and data standardization, in the {{env}} environment"` |
| `source_type` | string | **yes** | Which reader to use — see §2.2–§2.5. | `"autoloader"` |
| `target_catalog`, `target_schema`, `target_table` | string | **yes** each | Target Delta table's three-part name. | `"{{catalog}}"`, `"bronze_example"`, `"example_raw"` |
| `target_type` | string | **yes** | What kind of Lakeflow dataset to register — see [§4.1](#41-target_type-five-values). | `"streaming_table"` |
| `source_config` | object | no | Reader configuration; shape depends on `source_type`. §2.2–§2.5. | — |
| `target_config` | object | no | Target table + CDC configuration. §4. | — |
| `dq_config` | object | no | Row-level DQ rules + quarantine settings. §4.7. | — |
| `governance_tags` | object | no | Column/table tags. §4.8. | — |

There is no `source_type: "gcs_autoloader"` or top-level `source_format` field — see §2.3
for `source_type: "autoloader"` and `source_config.format`.

**Ingestion never allows `cdc_load_strategy: "SCD3"`** — `validate_spec` raises explicitly
when an ingestion flow's `target_config.cdc_load_strategy` is `SCD3`, because SCD3 pivots
current/previous state via an internal history table, which only makes sense downstream of
a raw ingestion flow (see [02_cdc_load_strategies.md §SCD3](02_cdc_load_strategies.md#scd3)).

### 2.2 `source_config` — fields common to every `source_type`

| Attribute | Type | Required | Description | Sample value | Allowed values |
|---|---|---|---|---|---|
| `capture_technical_metadata` | boolean | no (default `true`) | Attaches `__framework_source_file_name`/`__framework_source_file_size`/`__framework_source_file_modification_time`/`__framework_source_file_metadata_headers` (from Auto Loader's `_metadata` column — each field independently degrades to `NULL` rather than failing the read if the connector doesn't populate it) **and gates `__framework_ingestion_timestamp_utc`** (see [§4.5](#45-__framework_hash_key--__framework_hash_value--and-the-ingestion-timestamp)). | `true` | `true`, `false` |
| `schema_evolution_mode` | string | no | Auto Loader's `cloudFiles.schemaEvolutionMode`. | `"rescue"` | `"addNewColumns"`, `"addNewColumnsWithTypeWidening"`, `"rescue"`, `"failOnNewColumns"`, `"none"` |
| `file_pattern` | string | no | `cloudFiles.fileNamePattern` — a glob/regex scoping which files Auto Loader picks up from `path`. | `"orc_*"` | any glob/regex Auto Loader accepts |
| `reader_options` | object (string → string) | no | Passthrough to the underlying reader — one `.option(key, value)` call per entry, applied **after** `file_pattern`/schema-evolution options, so an entry here can override them if the keys collide. | `{"header": "true", "cloudFiles.inferColumnTypes": "true"}` | any Spark/Auto Loader reader option |
| `explode_columns` | array\<string\> | no | JSON struct/array flattening — see below. Runs unconditionally for every `source_type` (it's a post-read DataFrame transform, not a reader option). Empty/absent is a schema-preserving pass-through (no-op) unless `auto_flatten_all` is also set. | `["event_payload"]` | any list of top-level column names |
| `auto_flatten_all` | boolean | no (default `false`) | Opt-in only — takes effect when `explode_columns` is empty/absent, switching from the pass-through default to recursively flattening every struct column and exploding every array column anywhere in the schema. Ignored once `explode_columns` is populated. | `true` | `true`, `false` |
| `data_standardization_sql` | array\<string\> (restricted grammar) | no | Simple per-column standardization — see the runtime order note just below the table for exactly when this runs relative to `explode_columns`. | `["trim(region) AS region", "upper(country_code) AS country_code"]` | any single column-expression string ending `AS <column_name>` |
| `landing_retention_policy.clean_source` | string | no (default `"off"`) | Auto Loader's `cloudFiles.cleanSource` — what happens to a landing-zone file once ingested. | `"archive"` | `"archive"`, `"delete"`, `"off"` |
| `landing_retention_policy.archive_path` | string | **yes if** `clean_source = "archive"` | `cloudFiles.cleanSource.moveDestination`. | `"/Volumes/{{catalog}}/landing/_archive/example_raw_zone/"` | any Volume path |
| `landing_retention_policy.retention_days` | integer ≥ 0 | no | `cloudFiles.cleanSource.retentionDuration`, rendered as `"<N> days"`. | `7` | — |
| `normalize_column_names` | boolean | no (default `false`) | Opt-in trim/lowercase/replace-special-characters column-name cleanup. Full guide: [28_ingestion_schema_config.md](28_ingestion_schema_config.md). | `true` | `true`, `false` |
| `schema_config_path` | string | no | External JSON/YAML file (or directory, resolved to its latest-modified file) declaring explicit type casts, UC column comments, and source-to-target renames. Full guide: [28_ingestion_schema_config.md](28_ingestion_schema_config.md). | `"/Volumes/{{catalog}}/landing/_schema_configs/customer/"` | any file or directory path |

**Actual runtime order** (`notebooks/03_engine/03_lakeflow_declarative_pipeline.py`'s
ingestion path, after the reader itself returns a DataFrame): `apply_schema_config` (if
`schema_config_path` is set) → `normalize_column_names` (if enabled) → `attach_technical_metadata`
(the `_source_file_*` columns above) → `apply_explode_columns` → `apply_data_standardization_sql`
→ (back in the shared `register_staged_view`) `attach_framework_ingestion_timestamp` →
`encrypted_columns` → quarantine-column derivation. So `data_standardization_sql` expressions
can reference a column `explode_columns` just flattened into existence, and both run before
`__framework_ingestion_timestamp_utc`/encryption/quarantine ever see the row -- and every one
of them must reference the *final* column names, i.e. after `schema_config_path`/
`normalize_column_names` have already renamed anything they touch (see
[28_ingestion_schema_config.md](28_ingestion_schema_config.md) for why this exact order
matters).

`explode_columns` semantics (`ingestion/json_flattening.py::apply_explode_columns`):

* **Empty/absent**: recursively flatten *every* struct column (`address.city` →
  `address_city`) and explode *every* array column (one row per element, `explode_outer`
  so a `NULL`/empty array keeps the row rather than dropping it), anywhere in the schema,
  repeated up to 10 passes until nothing nested remains.
* **Populated**: scope the identical struct-flatten/array-explode treatment to exactly the
  named top-level columns. An `array<struct<...>>` column is exploded, then its resulting
  struct element is immediately flattened too — one entry (`"line_items"`) fully de-nests a
  line-items array in one step. A named column that resolves, at actual runtime, to neither
  a struct nor an array raises `FrameworkConfigError` — the onboarding-time validator only
  checks it's a list of strings, since it has no access to the source's real runtime schema.

`data_standardization_sql` (`ingestion/standardization_sql.py`) is a deliberately narrow
grammar, applied one `withColumn(alias, F.expr(expression))` per entry — never a full SQL
statement:

* Every entry must end with a literal `AS <column_name>` naming the output column — this is
  how the runtime knows which column to write, without re-deriving a name from the
  expression text.
* `_FORBIDDEN_STANDARDIZATION_KEYWORDS` rejects any bare, word-boundary occurrence of
  `SELECT`/`FROM`/`JOIN`/`UNION`/`WHERE`/`INSERT`/`UPDATE`/`DELETE`/`MERGE`/`DROP`/`ALTER`/
  `CREATE`/`GRANT`/`REVOKE` (case-insensitive), and a bare `;` — deliberately conservative:
  better to reject a legitimate edge case than silently accept a disguised full statement.
* This exact same grammar is reused, unchanged, by reconciliation's
  `source_config`/`target_configs[].data_standardization_sql` (§5.2) — one implementation,
  two call sites.

### 2.3 `source_type = "autoloader"`

Streaming Auto Loader (`cloudFiles`) ingestion. Reference:
[Auto Loader](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/index.html).
`path` (and every other Volume-backed path in this doc) points at a
[Unity Catalog Volume](https://docs.databricks.com/aws/en/connect/unity-catalog/volumes).

| Attribute | Type | Required | Sample value |
|---|---|---|---|
| `path` | string | **yes** | `"/Volumes/{{catalog}}/landing/example_raw_zone/incoming/"` |
| `format` | string | **yes** | `"csv"` (also `parquet`, `json`, `avro`, `text`, or any other `cloudFiles.format` Auto Loader supports — not itself restricted to an enum by `spec_validator.py`) |
| `schema_location` | string | no (auto-derived) | `"/Volumes/{{catalog}}/landing/_schemas/example_raw/"` |
| `source_zip_handling.*` | object | no | Same shape as the `asn1` reader's, §2.5 below. Extracts a (optionally PGP-decrypted) ZIP into `path`, at pipeline **execution** time, before Auto Loader ever reads it — all inside the same pipeline update, no separate job task. |

When `schema_location` is omitted, `_validate_ingestion_source_config` mutates
`source_config` in place, defaulting it to
`/Volumes/<target_catalog>/landing/_schemas/<target_table>/` — the same convention every
worked example in this repo already uses — and that derived value (not a placeholder) is
what gets persisted to the control table. Auto Loader's own `cloudFiles.schemaLocation`
option has no default of its own; this is the framework's addition. An explicit value
always wins if supplied.

### 2.4 `source_type = "zerobus"`

Streaming read of an existing Delta table — e.g. one landed by
[Zerobus](https://docs.databricks.com/en/ingestion/zerobus/index.html) direct-write ingest.
No file discovery at all, so `source_zip_handling`/`file_pattern`/`explode_columns` don't
apply here (there's no landing-zone file to unzip or flatten).

| Attribute | Type | Required | Sample value |
|---|---|---|---|
| `source_catalog`, `source_schema`, `source_table` | string | **yes** each | `"example_source_catalog"`, `"example_source_schema"`, `"example_source_zerobus_table"` |
| `starting_version` | integer | no | `0` (maps to `Reader.option("startingVersion", ...)`) |
| `max_bytes_per_trigger` | string | no | `"1g"` (maps to `Reader.option("maxBytesPerTrigger", ...)`) |

### 2.5 `source_type = "asn1"`

Binary telecom CDR ingestion, decoded via
[`asn1tools`](https://pypi.org/project/asn1tools/) (not a Databricks-native feature). File
discovery (Auto Loader `cloudFiles`, `binaryFile` format) and ASN.1 decoding
(`asn1/decoder.py`'s `mapInPandas` transform, which compiles the ASN.1 schema **once per
partition**, not once per row — a real perf fix over an earlier per-row UDF) are both fully
distributed across executors; no driver-side collection at any point.

| Attribute | Type | Required | Sample value |
|---|---|---|---|
| `path` | string | **yes** | `"/Volumes/{{catalog}}/landing/example_asn1_zone/extracted/"` |
| `schema_location` | string | no (auto-derived, same convention as `autoloader`) | `"/Volumes/{{catalog}}/landing/_schemas/example_asn1_cdr/"` |
| `asn1_schema_path` | string -- a real `.asn` ASN.1 module file, not a JSON wrapper | **yes** | `"/Volumes/{{catalog}}/landing/_asn1_schemas/example_cdr.asn"` |
| `asn1_codec` | string, one of `ber`/`der` | **yes** | `"ber"` |
| `asn1_pdu_name` | string -- the top-level `SEQUENCE` type to decode each record as | **yes** | `"ExampleCallDetailRecord"` |
| `source_zip_handling.enabled` | boolean | **yes**, must be a real bool | `true` |
| `source_zip_handling.source_zip_path` | string -- a **directory**, never a single file | **yes if enabled** | `"/Volumes/{{catalog}}/landing/example_asn1_zone/{{env}}/incoming/"` |
| `source_zip_handling.zip_file_pattern` | string (glob, same matching convention as `file_pattern`) | **yes if enabled** | `"example_cdr_batch.zip"` |
| `source_zip_handling.target_volume_path` | string | **yes if enabled** | `"/Volumes/{{catalog}}/landing/example_asn1_zone/extracted/"` |
| `source_zip_handling.pre_extraction_decryption` | object | no | Fully optional, absent or `{}` = plain, unencrypted ZIP. Decrypts the whole file **before** it's treated as a ZIP — see below |
| `source_zip_handling.pre_extraction_decryption.secret_passphrase` | object (`{secret_catalog, secret_schema, secret_key}`) | no (omit for a plain, unencrypted ZIP) | AES password on the ZIP itself, independent of `pre_extraction_decryption.type` — see [§4.3](#43-secrets-the-unity-catalog-three-level-shape) for the shared secret-ref shape |
| `source_zip_handling.delete_source_after_extract` | boolean | no (default `true`) | `true` |

`source_zip_handling` is idempotent across pipeline updates: once a batch has been
extracted (and, per `delete_source_after_extract`, removed), a later update finding the
source ZIP already gone logs and skips re-extraction rather than erroring — this is
expected steady-state, not a failure.

`pre_extraction_decryption` handles the common "PGP-encrypt, then ZIP" real-world shape —
the whole file is decrypted to a `.decrypted` temp path first, then handed unchanged to the
existing AES-ZIP-aware extraction. `type` (which dispatches this outer decryption layer) is
itself optional within the block — omit it entirely (or omit `pre_extraction_decryption`
altogether) when the file isn't wrapped in an outer decryption layer at all, i.e. it's just a
plain or AES-password-protected ZIP. When present, it's a **type-dispatched registry**
(`ALLOWED_PRE_EXTRACTION_DECRYPTION_TYPES = {"pgp"}` today), so a future algorithm is a new
registered handler, never a schema change:

```json
"pre_extraction_decryption": {
  "type": "pgp",
  "private_key_secret": {"secret_catalog": "{{catalog}}", "secret_schema": "security", "secret_key": "cdr_pgp_private_key"},
  "secret_passphrase": {"secret_catalog": "{{catalog}}", "secret_schema": "security", "secret_key": "cdr_zip_passphrase"}
}
```
(`secret_passphrase` here is the AES password on the ZIP archive itself — independent of
`type`, and combinable with it, as shown: PGP-decrypt the envelope first, then extract the
password-protected ZIP it contained. Omit `secret_passphrase` for a ZIP with no
archive-level password.)

`type: "pgp"` is the only currently-supported value; PGP encrypt/decrypt/sign uses `PGPy`
(pure Python, no external `gpg` binary — works on serverless compute) — see `crypto/pgp.py`.

`asn1_schema_path` points directly at a real ASN.1 module definition file — not a
hand-authored JSON field list. The Spark output schema (one column per member of
`asn1_pdu_name`) is derived automatically from that file via
`asn1/decoder.py::derive_asn1_field_defs` (`asn1tools.parse_files` introspection). Since
this is a plain `source_config` field, any `{{catalog}}`/`{{env}}` placeholder it contains
is substituted the same way `path`/`schema_location` are, before the pipeline ever runs.

```
ExampleCdrModule DEFINITIONS ::= BEGIN

ExampleCallDetailRecord ::= SEQUENCE {
    imsi                    IA5String,
    callDurationSeconds     INTEGER
}

END
```

Per [X.680](https://www.itu.int/rec/T-REC-X.680), ASN.1 identifiers allow letters, digits,
and hyphens but **not underscores**, so schema fields are camelCase
(`callDurationSeconds`), never snake_case. See
[01_control_metadata_schema.md §3](01_control_metadata_schema.md#3-source_config-ingestion-flows-only)
for the full ASN.1 → Spark type mapping table (nested `SEQUENCE`/`SEQUENCE OF` →
`struct`/`array`, `ENUMERATED` → the symbolic member name as a string, `CHOICE` not yet
supported, etc.).

---

## 3. `transformation_flows[]`

### 3.1 Fields common to every transformation flow

| Attribute | Type | Required | Description | Sample value |
|---|---|---|---|---|
| `flow_step_id` | string | **yes** | Unique id for this step; used to build the internal `_<target_table>_staged` view name. | `"ts_template_scd1_example"` |
| `dataflow_id` | string | **yes** | Which ingestion flow this transformation logically follows — a dependency-ordering label only; the real data dependency is `source_inputs[].table`, which can point at *any* table (not necessarily one this `dataflow_id` produced). | `"df_template_ingest"` |
| `target_catalog`, `target_schema`, `target_table` | string | **yes** each | Same as ingestion. | `"{{catalog}}"`, `"silver_example"`, `"example_dim_scd1_wide"` |
| `target_type` | string | **yes** | Same 5-value enum as ingestion — [§4.1](#41-target_type-five-values). | `"streaming_table"` |
| `transformation_sql` | string | **yes** | Native Spark SQL — §3.4. | `"SELECT * FROM example_dim_scd1_wide_src"` |
| `source_inputs` | array of objects | no | One or more upstream tables feeding `transformation_sql`. §3.2. | see below |
| `target_config` | object | no | Same shared shape as ingestion — §4. | — |
| `dq_config` | object | no | Same shared shape as ingestion — §4.7. | — |
| `governance_tags` | object | no | Same shared shape as ingestion — §4.8. | — |

Transformation flows allow every `cdc_load_strategy` ingestion allows **plus** `SCD3`
(transformation-only, since SCD3 pivots over an internal SCD2 history table built from
whatever `transformation_sql` already resolved).

### 3.2 `source_inputs[]`

Registers one `@dlt.view` per entry (`transformation/inputs.py::register_transformation_inputs`),
referenced by `input_name` directly in `transformation_sql`'s `FROM`/`JOIN`.

| Attribute | Type | Required | Description | Sample value |
|---|---|---|---|---|
| `input_name` | string | **yes** | **Must be unique across every `transformation_flows` entry in the whole spec, not just within one flow** — `_validate_no_duplicate_input_names` raises naming both flows if two flows reuse the same `input_name`. Every flow's `source_inputs` registers a view in the *same* pipeline graph (one `dataflow_group_id` = one graph), so there's no per-flow namespacing. | `"example_raw_join_left"` |
| `table` | string | **yes** | Fully-qualified upstream table — any table, not restricted to one this spec's own `ingestion_flows` produced. | `"{{catalog}}.bronze_example.example_raw"` |
| `is_streaming` | boolean | no (default `false`) | Reads via `spark.readStream.table(...)` vs. `spark.read.table(...)`. | `true` |
| `watermark.event_time_column` | string | **yes if streaming and joined with another stream** | Cast to `timestamp` before `withWatermark` is applied — a no-op if already a timestamp, but necessary because a JSON/CSV-sourced event-time column routinely arrives as `STRING`, and `withWatermark` requires a real `TimestampType` (a live-confirmed `EVENT_TIME_IS_NOT_ON_TIMESTAMP_TYPE` failure otherwise). | `"updated_at"` |
| `watermark.delay_threshold` | string | **yes if streaming and joined with another stream** | How late an event may arrive. | `"10 minutes"` |
| `decrypted_columns[]` | array of objects | no | Decrypts a source column before `transformation_sql` ever runs — §3.3. | 1 entry, `pii_column` → `pii_column_plain` |

Watermarks map onto `DataFrame.withWatermark(...)` — see
[Optimize stream processing with watermarking](https://docs.databricks.com/en/structured-streaming/watermarks.html)
and [stream-stream joins](https://docs.databricks.com/en/structured-streaming/stream-stream-joins.html)
for the native range-join pattern (`BETWEEN ... - INTERVAL ... AND ... + INTERVAL ...`) the
template's `ts_template_stream_join_watermark_example` flow uses.

### 3.3 `decrypted_columns[]` (`source_inputs[]` only)

**The only valid place to decrypt a source column** — never `target_config`, which only
ever encrypts SQL *output* columns (§4.3). Decryption happens per input, before
`transformation_sql` runs at all (`apply_aes_column_decryption`, called inside the view's
own build function).

```json
{
  "column_name": "ssn_encrypted",
  "output_column": "ssn",
  "cast_to_type": "string",
  "secret": {"secret_catalog": "{{catalog}}", "secret_schema": "security", "secret_key": "pii_encryption_key"}
}
```

| Attribute | Type | Required | Description | Allowed values |
|---|---|---|---|---|
| `column_name` | string | **yes** | Source (ciphertext) column — no safe default, never optional. | any column present on the input table |
| `output_column` | string | no (default: same as `column_name`) | Set to a new name to keep both ciphertext and plaintext side by side. | any column name |
| `mode` | string | no (default `"GCM"`) | AES cipher mode. **Must match the mode the column was originally encrypted with** — `aes_decrypt` isn't mode-agnostic. | `"GCM"`, `"CBC"`, `"ECB"` |
| `cast_to_type` | string | **yes** | Spark type to cast the decrypted plaintext to — decryption may change the physical type, so this is never inferred. | any Spark SQL type name (`"string"`, `"int"`, `"timestamp"`, ...) |
| `secret` | object (`{secret_catalog, secret_schema, secret_key}`) | **yes** | See [§4.3](#43-secrets-the-unity-catalog-three-level-shape). | — |

`cast_to_type` is cross-validated at **runtime** (not onboarding time — the check needs a
real Unity Catalog column tag to compare against) against an `original_data_type` tag the
framework captures automatically when the column was originally encrypted
(`crypto/column_crypto.py::apply_aes_column_encryption` returns a
`{output_column: original_spark_type}` map alongside the encrypted DataFrame — a planned,
**not yet wired**, post-deployment tag-application step; see the code comment in
`engine/flow_registration.py::register_staged_view` and
[16_encryption_and_secrets.md](16_encryption_and_secrets.md)). When that tag *is* present
and disagrees with `cast_to_type`, `apply_aes_column_decryption` raises `CryptoError`
naming the column, the configured `cast_to_type`, the tagged `original_data_type`, and the
fix (set `cast_to_type` to the tagged value, or re-encrypt with the type you actually want).

### 3.4 `transformation_sql`: parameters, UNION, and the `STREAM` keyword

Native Spark SQL, with three mechanical layers applied before it ever reaches `spark.sql(...)`:

1. **`${param}` substitution** (`transformation/parameters.py::substitute_dynamic_parameters`) —
   every `${key}` is replaced by `pipeline_parameters[key]`, rendered as a SQL literal
   (strings single-quoted with embedded quotes doubled; everything else via plain `str()`).
   A placeholder with no matching key raises `FrameworkConfigError` naming every missing
   key. **Never wrap `${param}` in your own quotes** — `WHERE country = '${filter_country}'`
   becomes `WHERE country = ''US''` (a `ParseException`) for a string parameter, because
   substitution already supplies the quotes; write `WHERE country = ${filter_country}`
   instead. (This exact mistake shipped in this repo's own early `spec_04`/`spec_06`
   fixtures until a real pipeline run caught it.)

   The same `${param}` syntax also works in `source_config`/`target_config` **path** fields
   (`source_config.path`/`schema_location`, `target_config.sink_config.path`/
   `post_export_archive.output_zip_path`, and the reconciliation equivalents in §5) — but
   via a *different* function, `substitute_path_parameters`, which renders every value with
   plain `str()` and never single-quotes it (quoting would corrupt a path). Don't confuse the
   two: a string parameter in `transformation_sql` comes back quoted, the same parameter used
   in a path does not. Not wired into `dq_config` or
   `observability_config.destination_config.volume_path`.
2. **`STREAM` keyword injection** (`transformation/inputs.py::mark_streaming_references`) —
   every bare `FROM`/`JOIN <input_name>` reference to a streaming input is rewritten to
   `FROM STREAM <input_name>`/`JOIN STREAM <input_name>` automatically (word-boundary
   matched, so a substring or unrelated occurrence of the name is never touched). Plain
   Spark SQL otherwise resolves a bare `FROM some_view` as a *batch* reference regardless of
   how the view was actually built — a live-confirmed `AnalysisException` (`'...' is a
   streaming view and must be referenced using readStream`) the first time any
   transformation flow's SQL referenced a streaming input, before this rewrite existed. You
   never write `STREAM` yourself — write ordinary SQL against `input_name` and let the
   engine add it.
3. **Onboarding-time syntax validation** (`onboarding/spec_validator.py::_validate_sql_syntax`) —
   the post-substitution SQL is run through `EXPLAIN <sql>` against a live Spark session. A
   `ParseException` (genuinely malformed SQL) is a hard onboarding error; an
   `AnalysisException` (e.g. a referenced input view doesn't exist yet, since the pipeline
   hasn't been deployed) is tolerated with a warning — it proves the SQL *parsed*
   correctly, even though it can't yet be resolved against a not-yet-deployed graph.

`UNION`/`UNION ALL` are fully supported — they parse and validate through the same
`EXPLAIN`-based path as any other SQL construct, across any combination of streaming and
batch inputs:

```json
"transformation_sql": "SELECT example_id, region, amount FROM example_union_left UNION ALL SELECT event_id AS example_id, NULL AS region, CAST(NULL AS DOUBLE) AS amount FROM example_union_right"
```

(`ts_template_union_all_example` in the template.)

---

## 4. `target_config`, `dq_config`, `governance_tags`

Shared, field-for-field identical structure between `ingestion_flows[]` and
`transformation_flows[]` — described once here.

### 4.1 `target_type` (five values)

| Value | Native Lakeflow object | Materialized table? | Which template/spec flow demonstrates it |
|---|---|---|---|
| `streaming_table` | `dlt.create_streaming_table` / `@dlt.table` (streaming) | yes | most flows in the template |
| `materialized_view` | Lakeflow materialized view (full recompute) | yes | `ts_template_truncate_and_load_example` |
| `batch_table` | `@dlt.table` (batch) | yes | ingestion flow 3 (`asn1`) — the **only** `target_type` where `storage_format: "iceberg"` is valid |
| `external_sink` | Real governed table (unchanged CDC dispatch) **plus** a second `@dlt.append_flow` export | yes, **and** exported | `ts_template_external_sink_example`, `ts_flagship_customer_scd1_egress` (`spec_24`) |
| `sink` | Genuine `dlt.create_sink` + `@dlt.append_flow`, fed directly by the staged view | **never** | `ts_template_pure_sink_example`, `ts_flagship_customer_direct_sink` (`spec_24`) |

Full mechanics, the streaming-only constraint, quarantine routing without a persisted
table, and every `sink_config` detail: [23_lakeflow_sinks.md](23_lakeflow_sinks.md). Quick
summary of the two sink types, since they're easy to conflate:

* **`"sink"`**: no main table is ever registered — the flow's staged `@dlt.view`
  (quarantine-filtered) feeds the sink directly. `cdc_load_strategy` is still required by
  the schema but is **functionally inert** here — it's never consulted for dispatch, only
  carried as a label on the structured log event. Requires a genuinely streaming source
  (Lakeflow sinks only support `@dlt.append_flow`, which is streaming-only) — a batch
  source raises `FrameworkConfigError` naming the exact fix.
* **`"external_sink"`**: registered exactly like `streaming_table`/`materialized_view`/
  `batch_table` (real, governed, CDC-dispatched table), **plus** a second `@dlt.append_flow`
  reading `dlt.read_stream(qualified_main_table)` into the sink. Requires the *main table*
  to be genuinely streaming — every CDC-dispatched strategy except `SCD3` publishes a real
  Lakeflow Streaming Table unconditionally, so `SCD3 + external_sink` always fails this
  check (SCD3's public target is a batch `@dlt.table` pivot — see
  [02_cdc_load_strategies.md §SCD3](02_cdc_load_strategies.md#scd3)); there's no
  configuration fix short of choosing a different strategy.

Both replace the old, now-deleted `control_plane/post_deployment.py::run_external_sink_exports`
— a separate post-deployment plain `spark.write` that raced against the pipeline and was
never actually part of its DAG.

### 4.2 `target_config` — full field table

**Every CDC-related field lives directly inside `target_config`** — there is no separate
`cdc_config` sibling.

| Attribute | Type | Required | Description | Sample value | Allowed values |
|---|---|---|---|---|---|
| `cdc_load_strategy` | string | **yes** | See [02_cdc_load_strategies.md](02_cdc_load_strategies.md). | `"SCD1"` | `APPEND`, `TRUNCATE_AND_LOAD`, `SCD1`, `SCD2`, `SCD3` (transformation only), `FULL_SNAPSHOT_CDC`, `FULL_SNAPSHOT_CDC_NO_PK` |
| `storage_format` | string | no (default `"delta"`) | `"iceberg"` is only valid when `target_type == "batch_table"` — a hard onboarding error otherwise. | `"delta"` | `"delta"`, `"iceberg"` |
| `partition_columns` | array\<string\> | no | Physical partitioning — **only takes effect for `APPEND`/`TRUNCATE_AND_LOAD`** targets; see the callout below. | `["region"]` | any column list |
| `liquid_clustering_columns` | array\<string\> | no | [Liquid clustering](https://docs.databricks.com/aws/en/delta/clustering) keys — **same `APPEND`/`TRUNCATE_AND_LOAD`-only caveat**, see below. | `["example_id"]` | any column list |
| `table_properties.log_retention_duration` | string | no | `delta.logRetentionDuration`. | `"interval 30 days"` | `"interval N days"` |
| `table_properties.deleted_file_retention_duration` | string | no | `delta.deletedFileRetentionDuration` (VACUUM safety window). | `"interval 30 days"` | `"interval N days"` |
| `table_properties.enable_iceberg_read_uniformity` | boolean | no | `delta.universalFormat.enabledFormats = iceberg` — Delta UniForm read-compat, for **any** `target_type` (unlike native `storage_format: "iceberg"`, batch-only). | `true` | `true`, `false` |
| `auto_ttl.timestamp_column` | string | no | `DATE`/`TIMESTAMP`/`TIMESTAMP_NTZ` column determining row age. **Only takes effect for `cdc_load_strategy` in `APPEND`/`TRUNCATE_AND_LOAD`**, see below. | `"updated_at"` | any such column present on the target |
| `auto_ttl.expire_in_days` | integer > 0 | no | Row-level TTL window. | `90` | any positive integer |
| `encrypted_columns[]` | array of objects | no; per-entry `column_name`/`secret` **yes** | Encrypts SQL **output** columns. §4.3. | 1 entry | — |
| `primary_keys` | array\<string\> | **yes for** `SCD1`/`SCD2`/`SCD3`/`FULL_SNAPSHOT_CDC` | Business key(s) for `apply_changes`/`apply_changes_from_snapshot`. Not required for `FULL_SNAPSHOT_CDC_NO_PK` (the framework supplies a surrogate key instead). | `["event_id"]` | any column list |
| `sequence_by_column` | string | no — **optional** for `SCD1`/`SCD2`/`SCD3` | Falls back to `__framework_ingestion_timestamp_utc` when omitted — see [§4.5](#45-__framework_hash_key--__framework_hash_value--and-the-ingestion-timestamp). | `"event_ts"` | any column name |
| `columns_to_check` | array\<string\> | no | Comparison-scoping; empty/absent = compare **every** applicable column. **Never include an encrypted column here** — AES-GCM's random IV makes identical plaintext re-encrypt to different ciphertext every run, which would look like a spurious change on every pipeline update. | `["amount"]` | any column list |
| `columns_to_exclude` | array\<string\> | no (`SCD1`/`SCD2`/`SCD3` only — flagged as meaningless otherwise) | Passed straight through to `dlt.apply_changes`'s native `except_column_list` — **the named columns are absent from the target table's schema entirely**, the practical way to onboard a wide source (e.g. 50 columns) without enumerating every column to keep. Also scopes hash/history comparison — see the important discrepancy noted in §4.4. | `["batch_load_ts", "source_extract_filename", "etl_run_id", "checksum_hash", "ingestion_notes"]` | any column list |
| `cdc_operation_column` | string | no (`SCD1`/`SCD2`/`FULL_SNAPSHOT_CDC`/`FULL_SNAPSHOT_CDC_NO_PK` only — flagged otherwise); required if `cdc_operation_mapping` is set | Column carrying an explicit insert/update/delete indicator. **Stays optional regardless of `primary_keys`** — a source can have a real business key with no explicit delete marker at all. | `"op"` | any column name |
| `cdc_operation_mapping.delete_values` | array\<string\> | **yes if** `cdc_operation_column` set | Values of that column meaning "this is a delete." | `["D"]` | any value list |
| `generate_hash_columns` | boolean | no (default `true` for any CDC-dispatched strategy) | Adds `__framework_hash_key`/`__framework_hash_value` — §4.5. | `true` | `true`, `false` |
| `generate_surrogate_key` | boolean | no (default `true` for `FULL_SNAPSHOT_CDC_NO_PK`, `false` otherwise) | Adds `__framework_surrogate_key` — §4.5. General-purpose: available to any ingestion/transformation/reconciliation flow, not just `FULL_SNAPSHOT_CDC_NO_PK`. | `true` | `true`, `false` |
| `capture_technical_metadata` | boolean | no (default `true`) | **Transformation flows only** (they have no `source_config` to host the ingestion-side field of the same name) — gates `__framework_ingestion_timestamp_utc` the same way `source_config.capture_technical_metadata` does for ingestion. | `true` | `true`, `false` |
| `sink_config` | object | **yes for** `target_type` in `{"sink", "external_sink"}` | §4.6. | — | — |

**Two non-obvious constraints worth calling out explicitly, since neither is visible from
the field table alone:**

* **`partition_columns`/`liquid_clustering_columns` only take effect for `APPEND`/
  `TRUNCATE_AND_LOAD` targets.** `dq/quarantine.py::register_main_and_quarantine_tables`
  only reads these two fields (into `partition_cols`/`cluster_by` kwargs on the
  `@dlt.table` decorator) in its non-CDC-dispatch branch. For any CDC-dispatched strategy
  (`SCD1`/`SCD2`/`SCD3`/`FULL_SNAPSHOT_CDC`/`FULL_SNAPSHOT_CDC_NO_PK`), the actual target
  table is created by `cdc/scd.py`/`cdc/snapshot.py`'s own `dlt.create_streaming_table(...)`
  calls, which never pass `partition_cols`/`cluster_by` at all — the fields validate and
  persist to the control table without error, but are silently inert on a CDC-dispatched
  target. If you need clustering on a CDC-dispatched target, `__framework_hash_key`
  clustering is applied automatically instead where relevant (see §4.5) — there's no
  configuration path to a *different* clustering key on such a target today.
* **`auto_ttl` only takes effect for `cdc_load_strategy` in `APPEND`/`TRUNCATE_AND_LOAD`.**
  Both `_validate_auto_ttl` (onboarding time) and `build_auto_ttl_kwarg`'s caller (runtime —
  same non-CDC-dispatch branch as above) enforce this: a fully-populated `auto_ttl` block on
  an `SCD1`/`SCD2`/etc. target is a **hard onboarding error** naming the flow's actual
  strategy, not merely inert. This is Auto TTL's own platform constraint (`ALTER TABLE ...
  DELETE ROWS` is explicitly unsupported for a Lakeflow streaming table — the only way to
  set/change it is the `auto_ttl=` decorator kwarg at table-creation time), not an arbitrary
  framework restriction.

`auto_ttl` is otherwise opt-in and individually forgiving: supplying only one of
`timestamp_column`/`expire_in_days` (or neither) is **not** a validation error — Auto TTL
simply isn't applied for that flow (a warning is logged). A *present-but-invalid* value
(e.g. `expire_in_days: 0`, or a non-identifier `timestamp_column`) is still a hard error.

### 4.3 Secrets: the Unity Catalog three-level shape

Every secret reference anywhere in this framework — encryption/decryption keys, ZIP
passwords, PGP keys, Kafka sink credentials — uses the identical shape, addressing a
[Unity Catalog secret](https://docs.databricks.com/aws/en/security/secrets/unity-catalog-secrets):

```json
{"secret_catalog": "{{catalog}}", "secret_schema": "security", "secret_key": "pii_encryption_key"}
```

Resolved via `dbutils.secrets.get(catalog=, schema=, key=)`
(`crypto/secrets.py::resolve_secret_value`) — **never** the SQL `secret(scope, key)`
function, which only resolves classic workspace-level scopes (a different, older system)
and cannot address a UC secret at all. A second, independent reason to avoid it: a
live-confirmed Databricks bug where credential-redaction machinery
(`spark.redaction.regex`, which matches the keyword `secret`) corrupts the *result* of any
query whose text contains a literal `secret(...)` call when materialized through Lakeflow's
own graph-definition/observability layer — 100% of encrypted values decoded back
byte-length-inflated and riddled with `U+FFFD` replacement characters, the unmistakable
signature of a lossy UTF-8 round trip. See
[16_encryption_and_secrets.md](16_encryption_and_secrets.md) for the full writeup; this is
why `resolve_secret_value` resolves outside Spark SQL entirely, and why every encryption/
decryption call site passes the resolved plaintext key through as an `F.lit(...)` literal
`Column`, never interpolated SQL text.

`encrypted_columns[]` entry shape (`target_config`, output-column encryption only):

```json
{
  "column_name": "pii_column",
  "output_column": "pii_column",
  "mode": "GCM",
  "secret": {"secret_catalog": "{{catalog}}", "secret_schema": "security", "secret_key": "pii_encryption_key"}
}
```

| Attribute | Type | Required | Description | Allowed values |
|---|---|---|---|---|
| `column_name` | string | **yes** | Plaintext source column — no safe default. | any column present in the flow's dataframe |
| `output_column` | string | no (default: same as `column_name`) | Output column name. | any column name |
| `mode` | string | no (default `"GCM"`) | AES cipher mode — `GCM` recommended (random IV; never use it in `columns_to_check`, see §4.2). | `"GCM"`, `"CBC"`, `"ECB"` |
| `secret` | object | **yes** | See above. | — |

`target_config.encrypted_columns` **only ever encrypts SQL output columns** — decrypting a
*source* column is `source_inputs[].decrypted_columns`'s job (§3.3), never
`target_config`'s. `apply_aes_column_encryption` also returns a
`{output_column: original_spark_type}` map, capturing each column's pre-encryption type —
consumed by the (planned, not-yet-wired) `original_data_type` UC column tag that
`decrypted_columns[].cast_to_type` cross-validates against (§3.3).

### 4.4 CDC field interactions — `columns_to_check` / `columns_to_exclude` / `primary_keys`

`cdc/comparison_columns.py::resolve_comparison_columns` is the single source of truth both
SCD2's native `track_history_column_list` and `__framework_hash_value` (§4.5) resolve
against:

```
excluded = columns_to_exclude ∪ primary_keys ∪ {framework technical columns}
base     = columns_to_check if non-empty else every column on the staged DataFrame
result   = sorted(base − excluded)
```

`FRAMEWORK_TECHNICAL_COLUMNS` (`__framework_ingestion_timestamp_utc`,
`__framework_hash_key`, `__framework_hash_value`, `__framework_surrogate_key`) is always
excluded automatically — these change on every run by construction (a timestamp, or a hash
of the very columns being compared) and would make every row look "changed" every time if
included.

**A documentation/implementation discrepancy worth flagging explicitly.** The module
docstring in `cdc/comparison_columns.py` (and, following it, `spec_validator.py`'s own
module docstring and [01_control_metadata_schema.md](01_control_metadata_schema.md))
states that `columns_to_exclude` is "comparison-only" and "no longer reaches
`dlt.apply_changes`'s `except_column_list`". **That is not what the code that actually runs
does.** `cdc/scd.py::_build_except_column_list` reads `target_config.get("columns_to_exclude")`
directly and `register_scd1`/`register_scd2`/`register_scd3` all pass it straight through as
`dlt.apply_changes(..., except_column_list=...)` — confirmed by reading all three functions
in full. `except_column_list` is a genuine `apply_changes` parameter that excludes those
columns from the **target table's schema**, not merely from comparison (this is also what
[02_cdc_load_strategies.md](02_cdc_load_strategies.md#scd1) independently describes, and
matches `_build_except_column_list`'s own docstring one function away: "Excluded columns
are simply absent from the resulting Delta table's schema"). So for `SCD1`/`SCD2`/`SCD3`,
`columns_to_exclude` in the currently-deployed code:

1. Removes the named columns from the target table's schema entirely, **and**
2. Also feeds `resolve_comparison_columns`, so an excluded column never triggers a
   spurious "changed" comparison either — moot for a column that's absent from the table
   anyway, but relevant if the same name is later re-added to `columns_to_check`
   accidentally.

Treat the "comparison-only, never drops columns" claim elsewhere in this repo's docs/code
comments as aspirational, not descriptive of the live behavior, until `cdc/scd.py` is
actually changed to stop building `except_column_list` from it (or the comment is
corrected) — this doc describes what the code you'll actually run does. Not fixed here per
this pass's documentation-only scope; flagged to the project owner separately.
`FULL_SNAPSHOT_CDC`/`FULL_SNAPSHOT_CDC_NO_PK` (`cdc/snapshot.py::register_full_snapshot_cdc`)
never wires `columns_to_exclude` into `apply_changes_from_snapshot` at all (it has no
`except_column_list` parameter to begin with) — consistent with `columns_to_exclude` being
validator-flagged as meaningless outside `SCD1`/`SCD2`/`SCD3` regardless of this
discrepancy.

### 4.5 `__framework_hash_key` / `__framework_hash_value` — and the ingestion timestamp

**Every CDC-dispatched flow** (`SCD1`/`SCD2`/`SCD3`/`FULL_SNAPSHOT_CDC`/
`FULL_SNAPSHOT_CDC_NO_PK`) gets two SHA-256 columns added to its target, computed
**centrally**, upstream of CDC dispatch (`dq/quarantine.py::_apply_hash_and_surrogate_key_columns`
— so `cdc/scd.py`/`cdc/snapshot.py` just consume already-present columns, never compute
their own):

* `__framework_hash_key` — `sha2(concat_ws('||', <coalesced, cast-to-string primary_keys>), 256)`.
* `__framework_hash_value` — same construction over the resolved comparison-column set
  (§4.4). Empty comparison set → constant hash for every row, not an error.

Gated by `generate_hash_columns` (default `true`). `__framework_surrogate_key`
(`crypto/hashing.py::generate_surrogate_key_hash`) is the same construction over *every*
non-audit column, gated by `generate_surrogate_key` (default `true` only for
`FULL_SNAPSHOT_CDC_NO_PK`, `false` elsewhere) — available to **any** flow type, including
plain `APPEND`/reconciliation flows, not just snapshot-CDC-without-a-key. When a surrogate
key is generated and no `primary_keys` are configured, it becomes the key hashed into
`__framework_hash_key` too.

`APPEND`/`TRUNCATE_AND_LOAD` targets never get these columns — there's no CDC comparison
concept for them. Every CDC-dispatched target additionally gets
`delta.enableChangeDataFeed=true`
([Delta Change Data Feed](https://docs.databricks.com/aws/en/delta/delta-change-data-feed),
`storage/table_properties.py`) so
`cdc/change_metrics.py::capture_scd_change_counts` can query `table_changes(...)` for exact
per-update inserted/updated/deleted counts. Reconciliation (§5) reuses precomputed hash
columns directly via `hash_precomputed: true` instead of recomputing them.

`__framework_ingestion_timestamp_utc` (`ingestion/technical_metadata.py`) is added to
**every** ingestion and transformation target — gated by `capture_technical_metadata`
(default `true`), idempotent-add (a no-op if the column already exists, e.g. inherited
through a join of two ingestion-sourced inputs). It is also the fallback sequencer for
`SCD1`/`SCD2`/`SCD3` when `sequence_by_column` is omitted, since `dlt.apply_changes` has no
concept of "no sequencer" at all.

### 4.6 `sink_config` (`target_type` in `{"sink", "external_sink"}`)

Required whenever `target_type` is `"sink"`/`"external_sink"`, identically for ingestion
and transformation flows. Full mechanics (the write/commit/abort split, why secrets are
resolved eagerly at graph-definition time rather than lazily inside `write()`/`commit()`,
the `kafka_secret_options` design rationale, extension points): see
[23_lakeflow_sinks.md](23_lakeflow_sinks.md). Field table:

| Attribute | Type | Required | Notes |
|---|---|---|---|
| `format` | string | **yes** | `"delta"`, `"kafka"`, or `"pgp_zip"`. |
| `path` | string | **yes for `delta`/`pgp_zip`** | Delta: target directory/table path. `pgp_zip`: the per-micro-batch **staging** directory for raw row files, *not* the finished archive location. Unused for `kafka`. |
| `kafka_options` | object (string→string) | **yes for `kafka`**, must include `kafka.bootstrap.servers` and `topic` | Same options a Spark Structured Streaming Kafka writer accepts. |
| `kafka_secret_options` | object (string → secret ref) | no | This framework's own addition, for a connector option needing a literal secret value with no Unity Catalog service-credential alternative — prefer `kafka_options["databricks.serviceCredential"]` when possible. |
| `post_export_archive.enabled` | boolean | **yes if `post_export_archive` present**; the block itself is required (and must be `true`) for `pgp_zip` | Archiving is the entire point of `pgp_zip`; structurally accepted but unused for `delta`/`kafka`. |
| `post_export_archive.output_zip_path` | string | **yes when `enabled`** | One finished archive file per micro-batch. |
| `post_export_archive.secret` | object (secret ref) | no | AES-256 password on the ZIP itself; omit for a plain, unencrypted ZIP. |
| `post_export_archive.pgp_encryption.enabled` | boolean | **yes if `pgp_encryption` present** | |
| `post_export_archive.pgp_encryption.recipient_public_key_secret` | object (secret ref) | **yes when `pgp_encryption.enabled`** | ASCII-armored PGP public key. |
| `post_export_archive.pgp_encryption.sign_with_private_key_secret` | object (secret ref) | no | Signs before encrypting when present. |

**A dead field to know about.** `sink_config.write_mode` (seen in the template's `"delta"`
example, `"append"`) validates fine (`check_dict`/`check_dict_of_str` accept unrecognized
keys) but is **never read** by `_build_sink_options`'s `"delta"` branch — a harmless
leftover from the old `target_config.sink_write_mode` field name. `@dlt.append_flow` is
always append-only; there is nothing to configure. Omit it.

Worked `"delta"` example — a pure `"sink"`, no materialized table
(`spec_06_unified_dual_engine_egress_zip.json`, `ts_iot_raw_events_direct_sink`):

```json
"target_config": {
  "cdc_load_strategy": "APPEND",
  "sink_config": {
    "format": "delta",
    "path": "/Volumes/{{catalog}}/egress/zips/iot_raw_direct_sink/{{env}}/"
  }
}
```

Worked `"pgp_zip"` example — `"external_sink"`, a real SCD1 table materialized **and**
PGP-encrypted-ZIP-exported (`spec_24_new_27_08_test_flagship.json`,
`ts_flagship_customer_scd1_egress` — live-verified in the dedicated `test_2026_08_07`
catalog):

```json
"target_config": {
  "cdc_load_strategy": "SCD1",
  "primary_keys": ["customer_id"],
  "generate_hash_columns": true,
  "sink_config": {
    "path": "/Volumes/{{catalog}}/egress/zips/flagship_customer_egress/{{env}}/_staging/",
    "format": "pgp_zip",
    "post_export_archive": {
      "enabled": true,
      "output_zip_path": "/Volumes/{{catalog}}/egress/zips/flagship_customer_egress_export/",
      "secret": {"secret_catalog": "{{catalog}}", "secret_schema": "security", "secret_key": "egress_zip_password"},
      "pgp_encryption": {
        "enabled": true,
        "recipient_public_key_secret": {"secret_catalog": "{{catalog}}", "secret_schema": "security", "secret_key": "egress_pgp_recipient_public_key"}
      }
    }
  }
}
```

`kafka` has no live-verified `test_specs/*.json` example in this repo yet — the shape below
is assembled directly from `_validate_sink_config`'s required-field checks:

```json
"sink_config": {
  "format": "kafka",
  "kafka_options": {
    "kafka.bootstrap.servers": "b-1.example-cluster.kafka.us-east-1.amazonaws.com:9096",
    "topic": "flagship_customer_events",
    "databricks.serviceCredential": "kafka_write_credential"
  },
  "kafka_secret_options": {
    "kafka.sasl.jaas.config": {"secret_catalog": "{{catalog}}", "secret_schema": "security", "secret_key": "kafka_sasl_jaas_config"}
  }
}
```

### 4.7 `dq_config`

```json
{
  "rules": [
    {"rule_id": "dq_example_id_not_null", "expression": "example_id IS NOT NULL", "action": "drop"},
    {"rule_id": "dq_amount_non_negative", "expression": "amount >= 0", "action": "quarantine"}
  ],
  "quarantine_table": "example_raw_quarantine",
  "record_id_column": "example_id"
}
```

| Attribute | Type | Required | Description | Sample value | Allowed values |
|---|---|---|---|---|---|
| `rules[].rule_id` | string | **yes** | Unique id, surfaced in `__framework_dq_failed_rule_ids`/`__framework_dq_failure_reasons`. | `"dq_example_id_not_null"` | any non-empty string |
| `rules[].expression` | string | **yes** | Boolean Spark SQL expression. | `"example_id IS NOT NULL"` | any boolean SQL expression |
| `rules[].action` | string | **yes** | `warn`/`drop`/`fail` map onto native Lakeflow expectations; `quarantine` is this framework's own extension. | `"quarantine"` | `"warn"`, `"drop"`, `"fail"`, `"quarantine"` |
| `quarantine_table` | string | no (default `"<target_table>_quarantine"`) | Moved here from `target_config`. | `"example_raw_quarantine"` | any table name |
| `record_id_column` | string | no | Surfaced as `__framework_record_id` on quarantined rows — moved here from `target_config`. | `"example_id"` | any column name present on the staged DataFrame |

**Quarantine table creation is conditional** — `register_main_and_quarantine_tables` only
registers `<target_table>_quarantine` (or `quarantine_table`'s override) when **at least
one** `rules[]` entry has `action == "quarantine"`. A `quarantine_table` name with no
quarantine-action rule is accepted (not an onboarding error — you may be about to add such
a rule) but produces **no table at all**: a real resource-waste bug fix over earlier
behavior that always created the quarantine table regardless. See
`test_specs/spec_27_quarantine_creation_and_non_creation.json` for a spec exercising both
outcomes side by side.

Every quarantined row carries: `__framework_dq_failed_rule_ids` (array of failing `rule_id`s),
`__framework_dq_failure_reasons` (array of human-readable `"<rule_id>: failed expression
'<expression>'"` strings), `__framework_dq_quarantine_flag`, `__framework_pipeline_run_id` (best-effort run/update
id, `NULL` if unavailable), `__framework_record_id` (from `record_id_column`, `NULL` if unconfigured or
absent), `__framework_source_file_name` (from `capture_technical_metadata`, when present), and
`__framework_quarantine_validated_at` (the processing timestamp, added at the quarantine table's own
closure). `warn`/`drop`/`fail` rules are evaluated separately, as native Lakeflow
expectations (`dq/expectations.py::apply_dq_expectations`) — only `quarantine`-action rules
drive the columns above.

### 4.8 `governance_tags`

**Tags-only model** — the framework applies key-value
[Unity Catalog tags](https://docs.databricks.com/aws/en/data-governance/unity-catalog/tags)
to columns and tables; it does **not** create or administer the Unity Catalog
masking/row-filter policy that gives a tag its actual enforcement behavior (a workspace
admin's tag-policy configuration, out of framework scope).

```json
{
  "column_tags": [
    {"column": "pii_column", "tags": {"mask": "PII", "classification": "restricted"}}
  ],
  "table_tags": {"row_filter": "region_restricted", "domain": "example"}
}
```

| Attribute | Type | Required |
|---|---|---|
| `column_tags[].column` | string | **yes** |
| `column_tags[].tags` | object (string→string, any number of tags) | **yes** |
| `table_tags` | object (string→string, any number of tags) | no |

Both support **multiple tags per column and per table** in one `SET TAGS` statement.
Applied post-deployment (never from inside the pipeline's own graph-definition code — `SET
TAGS` is Unity Catalog DDL against an already-materialized table) via `ALTER TABLE ... SET
TAGS (...)` / `ALTER TABLE ... ALTER COLUMN ... SET TAGS (...)`
(`governance/tags.py::apply_governance_tags`), both naturally idempotent, so **no
idempotency ledger is needed at all** — the old `governance/idempotency.py` and its
`governance_applied_log` table are deleted outright, not merely superseded. Every column's tags (and the table's
tags) are applied independently and best-effort — one failure doesn't block the others; all
failures are collected and raised together as one `AbacApplicationError`.

**A real, live-enforced example, worth knowing about if you're picking tag values for this
workspace.** This workspace has a genuine, live Unity Catalog tag policy restricting the
`mask` tag key to the values `PII`/`cost` only — confirmed by attempting an out-of-policy
value and observing the platform reject it. Every `mask` tag in this repo's specs
(`mask: "PII"`) respects that live constraint; it's a real example of tag-based governance
enforcement working end to end, not merely a label with no teeth.

---

## 5. `reconciliation_flows[]`

Full narrative, matching algorithm, streaming/batch mechanics, and worked examples:
[07_reconciliation.md](07_reconciliation.md). Field reference here.

### 5.1 Flow-level fields

| Attribute | Type | Required | Description | Sample value |
|---|---|---|---|---|
| `reconciliation_id` | string | **yes** | Unique id; also the key for `reconciliation_run_log` restartability. | `"recon_template_example"` |
| `source_config` | object | **yes** | The baseline dataset — shape in §5.2. | — |
| `target_configs` | array of objects, non-empty | **yes** | One or more comparison targets, each independently configured — §5.2/§5.3. **A list**, not a single object — one flow can compare its source against multiple targets. | 1 object in most specs |
| `match_keys` | array\<string\> | **yes** | Columns identifying the same logical record across every dataset in this flow. | `["example_id"]` |
| `compare_columns` | array\<string\> | no | Restrict drift comparison to specific columns; empty/omitted forces hash-equality to always pass (key-presence-only matching) — see [07_reconciliation.md](07_reconciliation.md#hash-first-matching-and-why). | `["amount", "status"]` |
| `generate_surrogate_key` | boolean | no | Same general-purpose mechanism as §4.5, for when neither side has a clean natural key. | `false` |
| `transform_sql` | string | no | Reshapes the miss set before append when source/target schemas differ — §5.4. | `"SELECT example_id, amount, status FROM missing_records"` |
| `error_handling.on_failure` | string | no (default `"fail"`) | Evaluated **per target**; allowed values `"fail"`/`"warn"`. | `"fail"` |

### 5.2 `source_config` / each `target_configs[]` entry — shared dataset shape

Both `source_config` and every `target_configs[]` entry validate through the same
`_validate_reconciliation_dataset_config`:

| Attribute | Type | Required | Description | Sample value | Allowed values |
|---|---|---|---|---|---|
| `type` | string | no (default `"table"`) | See [07_reconciliation.md §Type dispatch](07_reconciliation.md#type-dispatch-table--file--sink). | `"table"` | `"table"`, `"file"`, `"sink"` |
| `table` | string | **yes if** `type == "table"` | Fully-qualified `catalog.schema.table`. | `"{{catalog}}.bronze_example.example_volume_baseline"` | — |
| `path`, `format` | string | **yes if** `type` is `"file"`/`"sink"` | Raw location + Spark format. `type: "sink"` here means "read back what a `target_type: "sink"`/`"external_sink"` flow previously wrote" — unrelated to constructing a `dlt.create_sink`; identical read path to `"file"`. `path` may contain `${param}` placeholders, resolved (unquoted) from the parent group's `pipeline_parameters` on every run — see §3.4. | — | — |
| `read_mode` | string | no (default `"batch"`) | Independent per side — one flow can freely mix a streaming source against a batch target or vice versa. | `"batch"` | `"batch"`, `"streaming"` |
| `filter_condition` | string | no | Parameterized (`${param}`-substituted) Spark SQL predicate, applied via `.filter(...)` right after read, before matching ever sees the DataFrame. | `"load_date = '${run_date}'"` | any boolean SQL expression |
| `data_standardization_sql` | array\<string\> | no | Same restricted single-column-expression grammar as §2.2's ingestion field — same implementation, same rules. | `["trim(status) AS status"]` | — |
| `hash_precomputed` | boolean | no (default `false`) | Reuse an existing `__framework_hash_key`/`__framework_hash_value` instead of recomputing. **Only valid when `type == "table"`** — a file/sink location can't carry precomputed hash columns. | `true` | `true`, `false` |

### 5.3 `target_configs[]`-only fields

| Attribute | Type | Required | Description | Allowed values |
|---|---|---|---|---|
| `target_id` | string | **yes**, unique within the flow | Identifies this target in `reconciliation_run_log`/`reconciliation_mismatch_log`. | any non-empty string |
| `comparison_direction` | string | no (default `"both"`) | Gates which of the four classification outcomes get *acted* on — see [07_reconciliation.md §Four-way classification](07_reconciliation.md#four-way-classification-and-how-comparison_direction-gates-action). | `"source_to_target"`, `"target_to_source"`, `"both"` |
| `append_target_table` | string | **yes if** `comparison_direction` is `"source_to_target"` or `"both"` | Where self-healed records are idempotently appended. Never used, and safely omittable, for a `"target_to_source"`-only target (audit-only, nothing is ever written there). | any table |

```json
{
  "reconciliation_id": "recon_customer_master_volume_vs_cdc",
  "source_config": {"type": "table", "table": "{{catalog}}.bronze_recon_ops.raw_customer_master_baseline"},
  "target_configs": [
    {
      "target_id": "primary",
      "type": "table",
      "table": "{{catalog}}.bronze_recon_ops.raw_customer_master_cdc",
      "append_target_table": "{{catalog}}.bronze_recon_ops.raw_customer_master_cdc"
    }
  ],
  "match_keys": ["customer_id"],
  "compare_columns": ["status", "region"],
  "error_handling": {"on_failure": "fail"}
}
```

(`test_specs/spec_09_reconciliation_volume_vs_cdc.json`, live-verified end to end — see
[07_reconciliation.md §Worked example](07_reconciliation.md#worked-example-spec_09-end-to-end)
for the exact resulting `reconciliation_run_log`/`reconciliation_mismatch_log` rows.)

### 5.4 `transform_sql`

Unlike `data_standardization_sql`'s restricted grammar, `transform_sql` legitimately needs
full `SELECT`/`FROM` — joins, unions, column renames — to reshape a miss set from the
source's column layout into a target's, when the two schemas differ. Flow-level (applies
identically across every target in the flow), executed against a temp view named
**`_reconciliation_unmatched_records`** — every `transform_sql` must read `FROM
_reconciliation_unmatched_records`:

```json
"transform_sql": "SELECT customer_id, status, region FROM _reconciliation_unmatched_records WHERE region IS NOT NULL"
```

`${param}` resolves first (same mechanism as `filter_condition`), and, like
`transformation_sql`, it's syntax-validated at onboarding time via the identical
`EXPLAIN`-based check. `None`/empty is a pure passthrough — the miss set is appended with
its existing columns unchanged, the common case whenever source and `append_target_table`
already share a schema.

---

## 6. `observability[]`

A fifth, independent top-level array (alongside `ingestion_flows`/`transformation_flows`/
`reconciliation_flows`) declaring telemetry destinations for the standalone DLT observability
engine — a downstream Workflow task that re-exports the DLT event log as OpenTelemetry JSON
after a pipeline update completes (unrelated to this template's own ingestion/transformation
flows at the SQL/table level). Validated by `onboarding/spec_validator.py::
_validate_observability_destinations` and upserted by `onboarding/metadata_upsert.py::
upsert_observability_config` alongside every other flow in the same onboarding run; does
**not** count toward the "at least one flow array must be non-empty" requirement. See the
worked example at the end of `pipeline_onboarding_template.json`/`.yaml` and the complete
attribute dictionary in
[27_dlt_observability_onboarding_reference.md](27_dlt_observability_onboarding_reference.md) —
this doc deliberately doesn't duplicate that reference here.

---

## Relevant files

* `src/NextGen_Metadata_Framework/lakeflow_framework/onboarding/spec_validator.py` — the
  single source of truth this whole doc is built from.
* `onboarding_templates/pipeline_onboarding_template.json` / `.yaml` — the live-validated
  "kitchen sink" spec every **Sample value** above is copied from, unless a different
  `test_specs/*.json` file is named explicitly.
* `test_specs/spec_09_reconciliation_volume_vs_cdc.json`,
  `spec_24_new_27_08_test_flagship.json`, `spec_06_unified_dual_engine_egress_zip.json`,
  `spec_27_quarantine_creation_and_non_creation.json` — real, live-verified specs each
  exercising a cluster of the fields above end to end.
* `tests/unit/test_spec_loader.py::test_json_and_yaml_templates_parse_to_the_same_structure`
  — asserts the JSON and YAML templates parse to an identical dict.
