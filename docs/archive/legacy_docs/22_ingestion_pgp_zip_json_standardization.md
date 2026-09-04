# Ingestion: PGP/ZIP Archives, ASN.1 Distributed Decode, JSON Flattening, SQL Standardization

> See also: [README.md](README.md) — the full FlowX documentation set.

Four `source_config` capabilities, all shared between `autoloader` and `asn1` (except
where noted): archive-aware landing (`source_zip_handling`, with a type-registry
pre-extraction decryption step), Auto Loader file selection/passthrough (`file_pattern`,
`reader_options`), JSON struct/array flattening (`explode_columns`), and a deliberately
restricted column-standardization grammar (`data_standardization_sql`). This doc also
covers the `asn1` source's `mapInPandas` decode rewrite, since it's the other half of "what
happens to a file between landing and the target table" for that source type.

Full field reference (types, required/optional, allowed values) lives in
[01_control_metadata_schema.md §3](01_control_metadata_schema.md#3-source_config-ingestion-flows-only) —
this doc goes deeper on the *why* and the runtime mechanics behind four of those fields.
Source: `ingestion/readers.py`, `archive/zip_utils.py`, `crypto/pgp.py`,
`asn1/decoder.py`, `ingestion/json_flattening.py`, `ingestion/standardization_sql.py`.

---

## 1. `source_zip_handling` — decrypt, then unzip, before Auto Loader ever reads

Both `autoloader` and `asn1` sources accept an optional `source_zip_handling` block.
Enabled, it runs *inside* `read_autoloader_source`/`read_asn1_source` — i.e. at the
pipeline's actual execution time, not merely graph-definition time — so decrypt → unzip →
Auto Loader ingest → downstream transforms all happen within one Lakeflow Declarative
Pipeline update, no separate job task required (`ingestion/readers.py::_apply_source_zip_handling`).
`zerobus` doesn't get this option: it streams an existing Delta table, so there's no
landing-zone file to unpack.

`source_zip_path` is a **directory**, never a single file — a real landing zone routinely
accumulates more than one archive between pipeline updates (one per source extract/drop, or
several sibling flows sharing one incoming folder). `zip_file_pattern` (a glob, matched the
same way `file_pattern`/`cloudFiles.fileNamePattern` selects ingested files) selects which
archive(s) in that directory this update processes — an exact filename (`"cdr_batch.zip"`)
matches just one file; a wildcard (`"cdr_*.zip"`) matches every archive dropped under that
name pattern. Every match is decrypted/extracted independently, so one corrupt archive
doesn't block the others in the same update.

```json
"source_zip_handling": {
  "enabled": true,
  "source_zip_path": "/Volumes/{{catalog}}/landing/telecom_cdr_zone/incoming/",
  "zip_file_pattern": "cdr_batch.zip",
  "target_volume_path": "/Volumes/{{catalog}}/landing/telecom_cdr_zone/extracted/",
  "pre_extraction_decryption": {
    "secret_passphrase": {
      "secret_catalog": "{{catalog}}",
      "secret_schema": "security",
      "secret_key": "cdr_zip_passphrase"
    }
  },
  "delete_source_after_extract": true
}
```
(from `test_specs/spec_02_ingest_asn1_encrypted_zip.json`, an AES-password-protected ZIP
with no PGP layer — `pre_extraction_decryption.type` is omitted since there's no outer
decryption envelope; the block is present purely to carry `secret_passphrase`)

`enabled`/`source_zip_path`/`zip_file_pattern`/`target_volume_path` are required once the
block is present. `pre_extraction_decryption` is itself fully optional — absent entirely, or
present as `{}`, means a plain, unencrypted, non-password-protected ZIP. Nested inside it,
`secret_passphrase` (an ordinary three-level UC secret reference, resolved via
`crypto/secrets.py::resolve_secret_value` — see [16_encryption_and_secrets.md](16_encryption_and_secrets.md))
is the AES-256 passphrase on the ZIP container itself, via `pyzipper.AESZipFile` in
`archive/zip_utils.py::extract_encrypted_zip` — independent of, and combinable with, `type`
(§1.1 below), or usable entirely on its own as in the example above. Omit it for a plain,
unencrypted ZIP. `delete_source_after_extract` (default `true`) removes each ZIP once its
members are extracted, per the framework's landing-file lifecycle policy.

A real example of why the pattern matters: `test_specs/spec_16_bt_uc002_archive_autoloader_egress.json`
has four ingestion flows all landing into the same shared directory
(`/Volumes/BT_Group/landing/uc002_archive_zone/incoming/`) but each picking out only its own
archive by exact filename (`"bt_sales_north.zip"`, `"bt_sales_south.zip"`,
`"bt_ref_customers.zip"`, `"bt_ref_products.zip"`) — every flow scans the same directory but
extracts only what its own pattern matches.

**Idempotent across repeated pipeline updates.** Extraction only needs to run once per
archive: after the first successful update, `delete_source_after_extract` has removed each
matched ZIP, so once the directory has no more files matching `zip_file_pattern` (or the
directory itself is gone), a later update treats that as expected steady state (a log line,
not an error) rather than re-extracting or failing:

```
source_zip_handling: no files matching pattern '...' in '...' -- assuming already extracted
by a prior pipeline update (delete_source_after_extract) and skipping re-extraction.
```

### 1.1 `pre_extraction_decryption` — a type registry, not a schema field per algorithm

The real-world shape this handles: a file is **PGP-encrypted first, then ZIPped** (or the
reverse in transit — either way, the outermost layer received by the framework is not yet
a valid ZIP until it's decrypted). `pre_extraction_decryption`, nested inside
`source_zip_handling`, decrypts the whole file to a temporary path *before* it's ever
handed to the AES-ZIP-aware extractor. `type` (which dispatches this outer decryption layer)
is itself optional within the block — omit it entirely when the file isn't wrapped in an
outer decryption layer at all, as in the `secret_passphrase`-only example above:

```json
"pre_extraction_decryption": {
  "type": "pgp",
  "private_key_secret": {
    "secret_catalog": "{{catalog}}",
    "secret_schema": "security",
    "secret_key": "cdr_pgp_private_key"
  },
  "secret_passphrase": {
    "secret_catalog": "{{catalog}}",
    "secret_schema": "security",
    "secret_key": "cdr_zip_passphrase"
  }
}
```
(from `onboarding_templates/pipeline_onboarding_template.json`'s `df_template_asn1_ingest`
flow — PGP-encrypted, then a password-protected ZIP, both layers peeled in one pass: `type`
dispatches the outer PGP decrypt, and `secret_passphrase` — independent of `type`, just
co-located in the same block — is the AES password on the ZIP that PGP layer contained)

`type` dispatches through a small handler registry in `ingestion/readers.py`:

```python
_PRE_EXTRACTION_DECRYPTION_HANDLERS = {"pgp": _decrypt_pgp}
```

kept in sync with `onboarding/spec_validator.py::ALLOWED_PRE_EXTRACTION_DECRYPTION_TYPES =
{"pgp"}`. **Adding a future algorithm (e.g. a symmetric AES-file scheme with no ZIP layer at
all) means registering a new handler function and adding its name to that allowed-values
set — never restructuring `source_zip_handling`'s schema or the code path that calls it.**
An unrecognized `type` fails fast with a `FrameworkConfigError` naming the bad value and
listing the known types, both at onboarding-time validation and, defensively, again at
runtime dispatch.

**Decrypt-then-unzip order, and why it's fixed, not configurable:** `_apply_source_zip_handling`
resolves `source_zip_path` (the directory) + `zip_file_pattern` down to a list of matched
file paths, then hands each one to `_extract_one_zip_file`, which always decrypts first
(when `pre_extraction_decryption.type` is present — the block itself may be present purely
to carry `secret_passphrase`, which doesn't trigger this step) and unzips second — never the
other way around — because a PGP-encrypted payload is opaque ciphertext bytes, not a ZIP container;
`pyzipper.AESZipFile` can't open it until it's plaintext. Inside that per-file helper, the
decrypted bytes are written to `f"{source_zip_path}.decrypted"` (here `source_zip_path` is
the one matched file's own path, not the parent directory), and that path — not the
original — is what gets passed to `extract_encrypted_zip`:

```python
decryption_type = pre_extraction_decryption.get("type")
zip_path_to_extract = source_zip_path
if decryption_type:
    ...
    decrypted_bytes = handler(spark, encrypted_bytes, pre_extraction_decryption)
    zip_path_to_extract = f"{source_zip_path}.decrypted"
    with open(zip_path_to_extract, "wb") as decrypted_file:
        decrypted_file.write(decrypted_bytes)
...
extract_encrypted_zip(spark=spark, source_zip_path=zip_path_to_extract, ...)
```

Both the intermediate `.decrypted` file and the original encrypted source file are removed
in a `finally` block (the latter gated by `delete_source_after_extract`, same as the
no-PGP path) — the decrypted intermediate never lingers on the Volume regardless of
success or failure downstream.

The PGP decrypt call itself is `crypto/pgp.py::pgp_decrypt(data, private_key_armored,
passphrase=passphrase)`, built on [PGPy](https://pypi.org/project/pgpy/) — chosen
specifically because it's a pure Python wheel with no external `gpg` binary dependency
(`python-gnupg` shells out to a system `gpg`, not guaranteed present or safely invokable
across executors on Databricks serverless compute). `private_key_secret` is always required;
`passphrase_secret` is optional and independently configurable — a real, properly-secured
PGP private key is routinely passphrase-protected (this project's own throwaway test
keypairs were deliberately generated *without* one, so both paths need real coverage). Both
resolve from UC secrets via `crypto/secrets.py::resolve_secret_ref` immediately before
decryption. `pgp_decrypt` checks the key's `is_protected` flag itself: if the key is
protected but no `passphrase_secret` was configured, it raises `CryptoError` rather than
silently attempting an unlock. Any failure — bad key, wrong/missing passphrase, wrong
recipient, corrupt ciphertext — raises `CryptoError` wrapped in an `ArchiveError` naming the
source path and declared `type`.

```json
"pre_extraction_decryption": {
  "type": "pgp",
  "private_key_secret": {"secret_catalog": "{{catalog}}", "secret_schema": "security", "secret_key": "cdr_pgp_private_key"},
  "passphrase_secret": {"secret_catalog": "{{catalog}}", "secret_schema": "security", "secret_key": "cdr_pgp_private_key_passphrase"}
}
```
(`passphrase_secret` omitted entirely for a private key with no passphrase — this is the
common case in this project's own test keypairs)

### 1.2 Same shape on `autoloader`, not just `asn1`

`source_zip_handling` (including `pre_extraction_decryption`) is valid on `autoloader`
sources too. A CSV/JSON batch landed as a PGP-then-ZIP archive gets the identical decrypt →
extract → Auto-Loader-read pipeline, just with `cloudFiles.format` set to the tabular
format instead of `binaryFile`.

---

## 2. ASN.1: `mapInPandas`, compiled once per partition, not once per row

### 2.0 The output schema comes from the real `.asn` file, not a hand-authored field list

`asn1_schema_path` points directly at a real ASN.1 module definition file — `asn1_codec`
(`"ber"`/`"der"`) and `asn1_pdu_name` (the top-level `SEQUENCE` type to decode each record
as) are plain sibling `source_config` fields alongside it. There is no separate
`asn1_schema_json_path` wrapper file anymore: `asn1/decoder.py::derive_asn1_field_defs`
derives the Spark output field list automatically from `asn1_schema_path` via
`asn1tools.parse_files` introspection, resolving each member's ASN.1 type to a Spark type
recursively (nested `SEQUENCE` → `struct`, `SEQUENCE OF` → `array`, `ENUMERATED` → the
symbolic member name as a string, `CHOICE` explicitly unsupported — see
[01_control_metadata_schema.md §3](01_control_metadata_schema.md#3-source_config-ingestion-flows-only)
for the full mapping table). This also means `asn1_schema_path` — being a plain field in the
onboarding spec's own JSON/YAML text — gets the same `{{catalog}}`/`{{env}}` substitution as
`path`/`schema_location` always have, eliminating a real bug class the old external-JSON-file
design suffered from live (see [13_asn1_dq_quarantine.md](13_asn1_dq_quarantine.md)'s "Real
bug found via live deployment" note).

### 2.1 The bug this replaced

The original `asn1` decode step was a plain row UDF (`F.udf`) that called
`asn1tools.compile_files(...)` — which parses the `.asn` module text and builds an
in-memory codec — **inside** the per-row decode function. Spark has no way to know that
call is expensive and safely reusable across rows; it just sees "a Python function," so it
re-ran the full ASN.1 module compilation on every single row of every batch. For a CDR
stream processing millions of records, that's millions of redundant schema compilations
doing the exact same work over and over.

### 2.2 The fix: compile once per partition

`asn1/decoder.py::make_partition_decoder` builds a `mapInPandas` partition function
instead. Spark invokes the returned generator exactly once per partition, handing it an
iterator over that partition's pandas micro-batches — `asn1tools.compile_files(...)` runs
once, *before* the `for batch in batches` loop, and the one resulting compiled schema
object is reused for every row of every batch in that partition:

```python
def _decode_partition(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    compiled = asn1tools.compile_files(module_files, codec)
    for batch in batches:
        ...
        decoded = compiled.decode(pdu_name, raw_bytes)
        ...
        yield pd.DataFrame.from_records(records, columns=output_columns)
```

That cuts compilation from O(rows) to O(partitions) — a constant, small cost per partition
regardless of how many CDR records land in it. `make_partition_decoder` is also returned as
a plain, independently callable function specifically so a unit test can invoke it directly
against a handful of synthetic pandas DataFrames (no live Spark session needed) and
monkeypatch `asn1tools.compile_files` with a call-counting stub to *prove* the
once-per-partition behavior, rather than just asserting it by inspection.

Per-row decode failures (a malformed record, a codec mismatch) are still isolated per row —
caught and written to `_asn1_decode_error` (`f"{type(decode_exc).__name__}: {decode_exc}"`)
rather than raised, so one bad record in a partition doesn't fail the whole micro-batch.
Only a genuinely unloadable schema config (missing file, bad JSON, unsupported
`spark_type`) raises `Asn1DecodeError` — see [13_asn1_dq_quarantine.md](13_asn1_dq_quarantine.md)
for a worked example of routing `_asn1_decode_error IS NOT NULL` rows to quarantine.

### 2.3 Fully distributed, no driver-side collection, at either stage

Both halves of ASN.1 ingestion avoid the driver entirely, and the module docstring in
`asn1/decoder.py` says so explicitly:

> **Distributed by construction, end to end.** File discovery/reading happens via Auto
> Loader's `cloudFiles` source (`ingestion/readers.py::read_asn1_source`), which lists and
> reads files across executors natively — never collects file content to the driver. The
> decode step below runs via `DataFrame.mapInPandas`, which Spark also executes entirely on
> executors, one partition at a time, with no driver-side collection at any point.

Concretely: `read_asn1_source` opens a `cloudFiles`/`binaryFile` streaming reader over
`source_config["path"]` — Auto Loader's file listing and content reads both happen on
executors, the same as any other Auto Loader source — and hands the resulting raw-bytes
DataFrame straight to `decode_asn1_binary_stream(raw_df, ...)`, which attaches the
`mapInPandas` transform and returns a lazy DataFrame. Nothing in either step calls
`.collect()`, `.toPandas()`, or any other driver-materializing action; the decoded output
only actually executes when Lakeflow's own streaming engine pulls a micro-batch through the
graph.

One supporting detail: `mapInPandas` reconstructs the DataFrame from scratch across the
Python/Arrow boundary, which silently drops Auto Loader's hidden `_metadata` pseudo-column
if it was never materialized as a real column first. `decode_asn1_binary_stream` calls
`_materialize_hidden_metadata_column` before decoding specifically so downstream
`ingestion/technical_metadata.py::attach_technical_metadata` (which reads
`_metadata.file_name` etc.) doesn't silently degrade every ASN.1-sourced row's
`__framework_source_file_name`/`__framework_source_file_size` to `NULL`.

---

## 3. `file_pattern` and `reader_options` — shared Auto Loader passthrough

Both `autoloader` and `asn1` (which is itself a `cloudFiles.format("binaryFile")` reader
under the hood) route through the same helper,
`ingestion/readers.py::_apply_common_autoloader_options`. Both fields pass through to
[Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
itself, which does the actual file listing/reading:

```python
def _apply_common_autoloader_options(reader, source_config):
    file_pattern = source_config.get("file_pattern")
    if file_pattern:
        reader = reader.option("cloudFiles.fileNamePattern", file_pattern)
    for option_key, option_value in source_config.get("reader_options", {}).items():
        reader = reader.option(option_key, option_value)
    return reader
```

* **`file_pattern`** maps directly onto Auto Loader's `cloudFiles.fileNamePattern` — a
  glob/regex restricting which files in `path` are picked up, e.g. `"orc_*"` (from
  `onboarding_templates/pipeline_onboarding_template.json`'s `df_template_ingest` flow) to
  ingest only a specific file-naming convention out of a landing zone that may hold other
  files too.
* **`reader_options`** is an arbitrary string→string passthrough applied one
  `.option(key, value)` call at a time, in iteration order — every value the underlying
  Spark reader accepts (delimiter, header, quote, `cloudFiles.inferColumnTypes`, etc.) is
  fair game, with no framework-side allowlist:

```json
"reader_options": {
  "header": "true",
  "cloudFiles.inferColumnTypes": "true"
}
```

Both are applied *after* `_apply_landing_retention_policy` and evolution-mode options in
`read_autoloader_source`/`read_asn1_source`, but before `.load(path)` — plain reader
configuration, no DataFrame-level transform involved, so there's nothing to reorder or
worry about relative to `source_zip_handling` (which runs earlier, before the reader is
even constructed, since it operates on files at rest rather than the streaming read).

---

## 4. `explode_columns` — JSON struct/array flattening

Applied by the caller (the engine's ingestion staging step) as a post-read DataFrame
transform, not a reader option — `ingestion/json_flattening.py::apply_explode_columns`.
Driven by whether `explode_columns` is populated, plus one opt-in escape hatch:

**Empty or absent → schema-preserving pass-through by default.** The DataFrame is returned
unchanged — no struct/array column is touched unless it's named in `explode_columns`. Set
`source_config.auto_flatten_all: true` to opt into the old "just flatten this whole JSON
document" behavior instead: `_flatten_all` repeatedly scans the DataFrame's schema for any
remaining `StructType`/`ArrayType` column, flattens/explodes it, and loops (up to
`_MAX_FLATTEN_PASSES = 10`, a defensive bound not expected to matter for any real-world JSON
nesting depth) until no nested column remains. `auto_flatten_all` is ignored once
`explode_columns` is populated — it only governs the empty/absent case, and it defaults to
`false` precisely because recursive flattening on an un-configured source can silently
explode row counts (cartesian growth from exploding every array) and distort the target
schema. Prefer naming columns explicitly in `explode_columns` over `auto_flatten_all`
wherever the nested shape is known ahead of time.

**Populated → scope to exactly the named top-level columns.** Each entry must resolve, on
the *actual* DataFrame at runtime, to a struct or array type — checked here rather than at
onboarding time, since onboarding validation only confirms the field is a list of strings
(`check_list_of_str`) and has no access to the source's real runtime schema. An unresolvable
or wrong-type name raises `FrameworkConfigError`, naming the column and its actual type.

```json
"explode_columns": ["event_payload"]
```
(from `onboarding_templates/pipeline_onboarding_template.json`'s `df_template_json_ingest`
flow — flattens/explodes only `event_payload`, leaving every other top-level column
untouched)

**Struct flattening** replaces a struct column with one new top-level column per sub-field,
named `<column>_<subfield>` (so `address` with sub-field `city` becomes `address_city`).
**Array exploding** uses `explode_outer` (not `explode`) specifically so a row with a
null/empty array survives with a null element rather than being silently dropped —
exploding here is a shape transform for JSON ingestion, not an implicit row filter.

**Array-of-struct in one entry.** Naming a column whose type is `array<struct<...>>` first
explodes the array (one row per element), then immediately flattens the resulting struct
element too — so a single `explode_columns` entry like `"line_items"` fully de-nests a
`array<struct<sku, qty, price>>` column into top-level `line_items_sku`/`line_items_qty`/
`line_items_price` columns in one step, without a second entry or a second field naming the
post-explosion struct.

---

## 5. `data_standardization_sql` — a column-expression allowlist, deliberately more restricted than `transformation_sql`

`data_standardization_sql` (ingestion `source_config`, and the equivalent field on
reconciliation dataset configs) is validated by
`onboarding/spec_validator.py::_validate_data_standardization_sql` and applied at runtime
by `ingestion/standardization_sql.py::apply_data_standardization_sql`. Each entry is one
column expression, applied via `withColumn(alias, F.expr(expression))` — replacing an
existing column of that name, or adding a new one:

```json
"data_standardization_sql": [
  "trim(region) AS region",
  "upper(country_code) AS country_code"
]
```
(from `onboarding_templates/pipeline_onboarding_template.json`'s `df_template_ingest` flow)

### 5.1 The grammar, and why it's this restricted

Every expression **must** end with an explicit `AS <column_name>` clause — the runtime uses
that alias to know which output column to write to, rather than trying to re-derive a name
from the expression text:

```python
_ALIAS_PATTERN = re.compile(r"\bAS\s+`?([A-Za-z_][A-Za-z0-9_]*)`?\s*$", re.IGNORECASE)
```

The validator rejects, outright, any expression containing a bare occurrence of
`SELECT`/`FROM`/`JOIN`/`UNION`/`WHERE`/`INSERT`/`UPDATE`/`DELETE`/`MERGE`/`DROP`/`ALTER`/
`CREATE`/`GRANT`/`REVOKE` (case-insensitive, word-boundary matched), or a `;` anywhere in the
string:

```python
_FORBIDDEN_STANDARDIZATION_KEYWORDS = re.compile(
    r"\b(SELECT|FROM|JOIN|UNION|WHERE|INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|CREATE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)
```

This is deliberately more conservative than `transformation_sql`, which is a full SQL
statement — supports `UNION`/`UNION ALL`, joins across multiple `source_inputs`, arbitrary
`WHERE` clauses — and is validated by actually running Spark's `EXPLAIN` against it
(`onboarding/spec_validator.py::_validate_sql_syntax`) to catch a real syntax error before
onboarding, not merely to check its shape.

`data_standardization_sql` doesn't get that treatment because it isn't meant to be a
general SQL surface at all: it's a fixed, narrow per-column normalization step
(`trim(...)`, `upper(...)`, `substring(...)`, a bare column rename) applied identically to
every ingested row, one `withColumn` at a time, with no join and no row filtering in scope.
The keyword blocklist is a **defense against a disguised full statement** smuggled into
what's supposed to be a single expression — e.g. a value like
`"(SELECT secret_col FROM other_table) AS x"` or a stacked `"col; DROP TABLE x"` would, if
accepted, execute far more than a column-level transform inside `F.expr(...)`. Rejecting
any bare keyword occurrence is intentionally conservative — it will reject a handful of
legitimate edge cases (e.g. a column named `fromage` no longer trips it since matching is
word-boundary bound, but a genuine need to reference a *literal string* containing one of
these words as a substring would) in exchange for never accepting a disguised statement;
the validator's own comment states this trade-off explicitly.

A `;` is rejected unconditionally too — "exactly one column expression per entry," so even
a harmless-looking `"trim(x) AS x;"` is a validation error, not silently stripped.

### 5.2 What's *not* caught until runtime

Spark's lazy analysis means a **semantic** error — e.g. an expression referencing a column
that doesn't actually exist on the DataFrame — is not guaranteed to surface at onboarding
time; it appears once the DataFrame is actually executed inside the pipeline, as a plain,
clearly-attributable Spark error naming the bad reference. Only the *grammar* (single
expression, no forbidden keywords, resolvable `AS <column_name>`) is enforced up front by
the validator. A malformed expression that fails immediately in `F.expr(...)`/`withColumn`
at runtime is wrapped and re-raised as `FrameworkConfigError`, naming the offending
expression.
