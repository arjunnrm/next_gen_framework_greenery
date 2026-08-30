# Encryption and Secrets

> See also: [README.md](README.md) for the full Metaflow documentation set.

Every secret reference in Metaflow — AES encryption/decryption keys, PGP public/private
keys, ZIP archive passwords, Kafka sink credentials — is addressed as a genuine
[Unity Catalog secret](https://docs.databricks.com/aws/en/security/secrets/unity-catalog-secrets),
via the three-level `{secret_catalog, secret_schema, secret_key}` shape, and resolved
exclusively via `dbutils.secrets.get(catalog=, schema=, key=)`. See
[01_control_metadata_schema.md](01_control_metadata_schema.md) for how these fields sit
inside `encrypted_columns`/`decrypted_columns`/`source_zip_handling`/`sink_config`.

Implemented in `src/NextGen_Metadata_Framework/lakeflow_framework/crypto/`:

* `secrets.py` — identifier safety (`assert_safe_identifier`), UC secret resolution
  (`resolve_secret_value`/`resolve_secret_ref`).
* `column_crypto.py` — `apply_aes_column_encryption`/`apply_aes_column_decryption`
  (`aes_encrypt`/`aes_decrypt` via native PySpark column functions, never SQL text).
* `pgp.py` — `pgp_encrypt`/`pgp_decrypt`/`pgp_verify` via `PGPy`.
* `hashing.py` — `generate_surrogate_key_hash`, the general-purpose
  `__framework_surrogate_key` generator. Despite living in this package, it has nothing to
  do with secrets or encryption — it hashes payload columns with no key material at all.
  Covered in [02_cdc_load_strategies.md](02_cdc_load_strategies.md) and
  [01_control_metadata_schema.md §4a](01_control_metadata_schema.md#4a-hash-keyvalue--a-framework-design-principle),
  not here.

Two consumers outside `crypto/` complete the picture and are covered in full below:
`engine/sink_registration.py` (builds `dlt.create_sink(...)` options, including every
secret an egress sink needs) and `archive/pgp_zip_sink.py` (the custom Lakeflow sink that
actually writes PGP+ZIP archives, and the reason §6 exists).

---

## 1. The secret reference shape: `{secret_catalog, secret_schema, secret_key}`

One shape, used everywhere a config needs a secret:

```json
{"secret_catalog": "poc", "secret_schema": "security", "secret_key": "pii_encryption_key"}
```

This addresses a secret at `poc.security.pii_encryption_key` in the
[Unity Catalog secrets model](https://docs.databricks.com/aws/en/security/secrets/unity-catalog-secrets)
— a genuinely different, newer system from a classic workspace-level secret scope, not just
a different spelling of the same thing (see §10). It's validated everywhere by the same
function, `onboarding/spec_validator.py::check_secret_ref`, which requires all three of
`secret_catalog`/`secret_schema`/`secret_key` as non-empty strings whenever a secret
reference is itself required.

Every location this shape appears, and whether it's required:

| Location | Field | Required? |
|---|---|---|
| `target_config.encrypted_columns[]` | `.secret` | **yes**, per entry |
| `source_inputs[].decrypted_columns[]` | `.secret` | **yes**, per entry |
| `source_zip_handling.pre_extraction_decryption` | `.secret_passphrase` (AES password on the ZIP itself, independent of `type`) | no — omit for a plain, unencrypted ZIP |
| `source_zip_handling.pre_extraction_decryption` | `.private_key_secret` | **yes** when `type == "pgp"` |
| `target_config.sink_config.kafka_secret_options` | `.<option_key>` (a dict of secret refs, one per Kafka connector option) | **yes**, per entry, when present |
| `target_config.sink_config.post_export_archive` | `.secret` (AES password on the egress ZIP) | no — omit for a plain ZIP |
| `post_export_archive.pgp_encryption` | `.recipient_public_key_secret` | **yes** when `pgp_encryption.enabled` |
| `post_export_archive.pgp_encryption` | `.sign_with_private_key_secret` | no — omit to encrypt without signing |

`resolve_secret_ref(spark, secret_ref)` (`crypto/secrets.py`) is the single convenience
wrapper every one of these locations resolves through — see §2.

---

## 2. Resolving a secret: `dbutils.secrets.get()`, never the SQL `secret()` function

```python
def resolve_secret_value(spark, secret_catalog, secret_schema, secret_key) -> str:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    return dbutils.secrets.get(catalog=secret_catalog, schema=secret_schema, key=secret_key)
```

That's the entire resolution path (`crypto/secrets.py::resolve_secret_value`) — constructed
from `spark` per Databricks' documented pattern for reaching `dbutils` from library code (not
notebook-top-level code), because `dbutils.secrets.get(catalog=, schema=, key=)` is the
**only** supported resolution API for a Unity Catalog secret; there is no SQL-callable
equivalent for it. A malformed catalog/schema/key, a nonexistent secret, or a caller missing
the `READ SECRET` grant all surface as `SecretResolutionError` naming the fully-qualified
`catalog.schema.key` label.

Before either the catalog/schema/key ever reaches `dbutils`, `assert_safe_identifier`
(`crypto/secrets.py`) rejects anything outside `[A-Za-z0-9_]` — deliberately stricter than
what Unity Catalog itself allows for a secret catalog/schema/key name, because these values
are metadata-driven (sourced from a control-table row written from onboarding JSON, not a
hardcoded literal) and this is the guard that closes off SQL-injection before any of them
gets spliced into DDL elsewhere in the framework (governance tag statements, quarantine
table names, etc.) — one shared identifier-safety boundary, not a secrets-specific one.

Requires **Databricks Runtime 17.3 LTS+ or serverless environment version 4+** — Unity
Catalog secrets' minimum supported runtime.

### Why not the SQL `secret()` function — a real, live-confirmed redaction bug

The original implementation of `crypto/column_crypto.py` built its encryption expression as
one interpolated SQL string — `aes_encrypt(CAST(col AS STRING), secret('scope', 'key'),
'MODE')` via `F.expr(...)` — specifically to keep the resolved key value off the Python
driver. Confirmed live, this **silently corrupts the resulting ciphertext**: every encrypted
value decoded back to a byte-length-inflated string riddled with `U+FFFD` replacement
characters, the unmistakable signature of a lossy UTF-8 decode/re-encode round trip. This
was invisible to the framework's original encryption test (which only ever confirmed
"not plaintext," never actually decrypted and round-tripped a value) until a later exhaustive
suite added a genuine round-trip assertion.

Root cause: Databricks' platform-wide credential-redaction machinery (`spark.redaction.regex`,
which matches the keyword `secret` among others) treats any expression whose *query text*
contains a `secret(...)` call as credential-bearing and redacts/mutates it as part of
Lakeflow's per-dataset metrics/observability pipeline — confirmed against a documented,
Databricks-support-acknowledged issue with the exact same symptom:
[Redacted possible secret access key as part of column value](https://community.databricks.com/t5/data-engineering/redacted-possible-secret-access-key-as-part-of-column-value/td-p/55086).
Isolated, non-Lakeflow Structured Streaming writes using identical inline-`secret()` SQL
text never reproduced this — it's specific to Lakeflow's own logging/observability layer,
not to `aes_encrypt`/`secret()` in general. A second, related manifestation hit
`resolve_secret_value`'s own resolution query when that function was still SQL-based, before
it moved to `dbutils.secrets.get()` entirely.

The fix, per Databricks support's own confirmed workaround, is what `column_crypto.py` does
today: resolve the secret's plaintext value once via `resolve_secret_ref` *before* building
the encryption/decryption expression, then pass it into PySpark's native `F.aes_encrypt`/
`F.aes_decrypt` column functions as a literal `Column` (`F.lit(resolved_key)`), never as
interpolated SQL text. No `secret(` substring ever appears in the expression driving the
write, so the redaction trigger can't fire. Unity Catalog secrets have no SQL-callable
resolution function at all (`dbutils.secrets.get()` is the only path), so this specific class
of bug cannot recur here even in principle — but the underlying lesson (never let a literal
`secret(...)`-shaped substring reach a Lakeflow graph-definition query's text) is worth
remembering if a future contributor is ever tempted to reintroduce a SQL-text shortcut.

**Trade-off this accepts:** the resolved secret's plaintext value now exists as a Python
string on the driver for the duration of one flow's graph-definition/execution call — an
intentional trade against the alternative of encryption being silently non-functional.

---

## 3. Column-level AES encryption and decryption

`crypto/column_crypto.py`'s two functions are symmetric in shape but live in different
places in the schema, and that placement is meaningful:

* **`target_config.encrypted_columns[]`** (`apply_aes_column_encryption`) — output-column
  encryption only, on both ingestion and transformation flows. Encrypts a *SQL output*
  column; it is never used to decrypt a source column.
* **`source_inputs[].decrypted_columns[]`** (`apply_aes_column_decryption`) — the **only**
  valid place to decrypt a column, and only on transformation flows (ingestion flows have no
  `source_inputs`). Runs per source input, before `transformation_sql` ever executes.

Both entries share the same base shape:

```json
{
  "column_name": "ssn_raw",
  "output_column": "ssn_encrypted",
  "mode": "GCM",
  "secret": {"secret_catalog": "poc", "secret_schema": "security", "secret_key": "pii_encryption_key"}
}
```

| Field | Required | Notes |
|---|---|---|
| `column_name` | **yes** | Must exist in the DataFrame at the point encryption/decryption runs. |
| `output_column` | no (defaults to `column_name`) | Set it to a different name to keep both the ciphertext and plaintext columns side by side. |
| `mode` | no (defaults `"GCM"`) | `"GCM"`, `"CBC"`, or `"ECB"` — `ALLOWED_AES_MODES` in the validator. |
| `secret` | **yes** | The three-level UC secret reference from §1. |
| `cast_to_type` | **yes, decryption only** | See §4 — required because decryption can change the physical column type. |

`column_name`/`secret` have no default on either side: guessing which column to encrypt, or
which key to use, is a security decision this framework will not make silently — a missing
one is a hard `CryptoError` at flow-registration time, not a warning.

A realistic encrypt-then-decrypt-then-re-encrypt round trip, trimmed from the live-verified
"New 27_08 Test" flagship pipeline (`test_specs/spec_24_new_27_08_test_flagship.json`) and
the "kitchen sink" template's SCD2 example (`onboarding_templates/pipeline_onboarding_template.json`):

```json
// Bronze ingestion (target_config.encrypted_columns) — encrypts the raw column at rest
{
  "target_config": {
    "cdc_load_strategy": "APPEND",
    "encrypted_columns": [
      {
        "column_name": "ssn",
        "output_column": "ssn",
        "mode": "GCM",
        "secret": {"secret_catalog": "{{catalog}}", "secret_schema": "security", "secret_key": "pii_encryption_key"}
      }
    ]
  }
}

// Silver transformation (source_inputs[].decrypted_columns) — decrypts it for use in transformation_sql
{
  "source_inputs": [
    {
      "input_name": "customer_raw_stream",
      "table": "{{catalog}}.bronze_flagship.customer_raw",
      "is_streaming": true,
      "decrypted_columns": [
        {
          "column_name": "ssn",
          "output_column": "ssn_plaintext",
          "cast_to_type": "string",
          "secret": {"secret_catalog": "{{catalog}}", "secret_schema": "security", "secret_key": "pii_encryption_key"}
        }
      ]
    }
  ],
  "transformation_sql": "SELECT customer_id, customer_name, country, tier, ssn_plaintext, __framework_ingestion_timestamp_utc FROM customer_raw_stream"
}
```

The template's SCD2 example goes one step further and re-encrypts the decrypted column
(`pii_column_plain`) right back into `target_config.encrypted_columns` before it lands on
the Silver target — a legitimate pattern (decrypt only long enough to transform, re-encrypt
before persisting), and proof the two mechanisms compose freely across a flow.

**AES-GCM has a random IV per call — never put an encrypted column in `columns_to_check`.**
`GCM` (the default and recommended mode) re-encrypts identical plaintext to different
ciphertext on every call, so an SCD1/SCD2/SCD3 change-comparison over an encrypted column
would see a "change" on every single row, every single run, even when nothing actually
changed. Keep encrypted columns out of `target_config.columns_to_check`, or exclude them via
`columns_to_exclude` — see [02_cdc_load_strategies.md](02_cdc_load_strategies.md) and
[10_test_pipeline_2_streaming_cdc.md](10_test_pipeline_2_streaming_cdc.md).

**AES key material must be exactly 16, 24, or 32 raw bytes** — `aes_encrypt`/`aes_decrypt`
reject anything else with `INVALID_PARAMETER_VALUE.AES_KEY_LENGTH`. A base64-encoded
32-byte value is 44 *characters* as a string and fails this check; `openssl rand -hex 16`
(32 hex characters == 32 bytes when stored as a string) is a valid AES-256 key. This
constraint is about the resolved *value* `aes_encrypt` receives, not where that value is
stored or how it's addressed.

---

## 4. `original_data_type` type validation — current implementation status

The intended design, and what's actually wired today, are not the same thing yet — worth
being explicit about rather than documenting the aspiration as if it were shipped.

**The design:** `apply_aes_column_encryption` returns `(df, {output_column:
original_spark_type})` — the pre-encryption Spark type of every column it just encrypted
(encryption replaces a column's physical type with ciphertext binary, so this is the only
place that original type is still recoverable). The intent is for the caller to persist that
map as a Unity Catalog `original_data_type` column tag once the target table is
materialized, so that a downstream `decrypted_columns[].cast_to_type` can be validated
against ground truth rather than trusted on faith. `apply_aes_column_decryption` already
supports the read side of this: it accepts an optional `original_type_tags` map
(`{column_name: original_data_type}`), and when a tagged type is present and disagrees with
the configured `cast_to_type`, it raises:

```python
raise CryptoError(
    f"decrypted_columns cast_to_type mismatch for column '{column_name}': configured "
    f"cast_to_type={cast_to_type!r} but the encrypted column's tagged original_data_type is "
    f"{tagged_type!r}. Fix: set cast_to_type to {tagged_type!r} (or re-encrypt the source "
    "column with the type you actually want decrypted_columns to produce)."
)
```

— naming the column, the configured `cast_to_type`, the tagged `original_data_type`, and a
concrete fix, rather than silently producing a wrongly-typed column.

**What's actually wired right now:** the write side is not connected. In
`engine/flow_registration.py::register_staged_view`, the `original_types` map
`apply_aes_column_encryption` returns is captured and then deliberately discarded
(bound to `_original_types`, never used) — the closure it runs inside executes lazily, at
Lakeflow graph-*execution* time, while tagging the target table can only happen once that
table is actually materialized, a separate, later step that doesn't exist yet. And on the
read side, `transformation/inputs.py` calls `apply_aes_column_decryption(view_df,
decrypted_columns)` with no third argument, so `original_type_tags` is always `{}` in
production — the mismatch check above is real, tested code, but it never actually fires
today, because nothing ever supplies it a tagged type to compare against.

**Practical consequence for spec authors:** `decrypted_columns[].cast_to_type` is still a
hard-required field — the validator rejects a spec that omits it — but nothing currently
cross-checks the value you supply against the column's real pre-encryption type. Get it
right by hand (match it to whatever type the source column had before
`target_config.encrypted_columns` encrypted it). Persisting the `original_data_type` UC
column tag post-materialization, and reading it back before `source_inputs[].decrypted_columns`
runs, is tracked follow-up work, not a bug to route around — this note is the tracking
marker `engine/flow_registration.py`'s own code comment points at.

---

## 5. PGP: encrypt, decrypt, and sign via PGPy

`crypto/pgp.py` wraps [`PGPy`](https://pypi.org/project/PGPy/) — a pure-Python OpenPGP
implementation with **no external `gpg` binary dependency**, chosen deliberately over
`python-gnupg` (which shells out to a system `gpg` binary not guaranteed present, or safely
invokable across executors, on Databricks serverless compute). It works identically on
serverless and classic clusters as a normal PyPI wheel dependency.

Three functions, all taking **already-resolved** key material (ASCII-armored PEM text) as
plain arguments — none of them ever calls `resolve_secret_ref`/`dbutils` itself:

| Function | Signature | Notes |
|---|---|---|
| `pgp_decrypt` | `(data: bytes, private_key_armored: str, passphrase: Optional[str]) -> bytes` | Passphrase required only if the private key is itself passphrase-protected. |
| `pgp_encrypt` | `(data: bytes, recipient_public_key_armored: str, sign_with_private_key_armored: Optional[str], sign_passphrase: Optional[str]) -> bytes` | Sign-then-encrypt when a signing key is supplied. Returns ASCII-armored ciphertext. |
| `pgp_verify` | `(data: bytes, signed_message: bytes, signer_public_key_armored: str) -> bool` | Returns `False` for a clean "signature doesn't match," raises `CryptoError` for a parse/verification error. |

Keys are **always** resolved via a Unity Catalog secret first — never a literal key file
path or inline key material in an onboarding spec. There are two call sites in this
framework, and they resolve their keys at very different points for very different reasons:

**a. Ingestion-side: `source_zip_handling.pre_extraction_decryption`.** The common
real-world shape is "PGP-encrypt, then ZIP" (or the reverse) — the whole landed file is
PGP-decrypted to a temp path first, then handed unchanged to the existing AES-ZIP-aware
extractor. It's a type-dispatched registry (`{"type": "pgp", "private_key_secret": {...}}`;
`"pgp"` is the only registered handler today — see
[01_control_metadata_schema.md §3](01_control_metadata_schema.md#source_type--asn1)) so a
future algorithm is a new handler, never a schema change. This runs inside
`ingestion/readers.py::_apply_source_zip_handling`, called from within
`read_autoloader_source`/`read_asn1_source` — i.e. at the pipeline's actual *execution* time
(not merely graph-definition time), on the **normal pipeline driver notebook process**,
where `dbutils` works exactly as documented. `resolve_secret_ref` is called directly, right
where the value is needed — the general pattern every other secret use in this framework
follows, and the one §6 below is the deliberate exception to.

**b. Egress-side: `sink_config.post_export_archive.pgp_encryption`.** Encrypts (and
optionally signs) the finished ZIP archive produced by the `pgp_zip` custom sink format
before it's written to `output_zip_path`. This is where the eager-resolution constraint in
§6 applies — read on.

---

## 6. The eager-secret-resolution constraint inside the `pgp_zip` custom sink

This is a real, load-bearing platform constraint, confirmed live, not an implementation
detail to gloss over. Get it wrong and encryption silently fails at pipeline runtime, deep
inside a code path that never gets exercised until a real deployment.

### What a spec author writes

Trimmed from the live-verified "New 27_08 Test" flagship pipeline
(`test_specs/spec_24_new_27_08_test_flagship.json`) — an `external_sink` flow whose
`sink_config.format` is `pgp_zip`, so its egress ZIP gets an AES-256 password on top of a
PGP-encryption layer:

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

Three `{secret_catalog, secret_schema, secret_key}` references sit in that block
(`post_export_archive.secret`, `.pgp_encryption.recipient_public_key_secret`, and the unused-here
optional `.pgp_encryption.sign_with_private_key_secret`) — none of them resolved yet at the
point `onboarding/spec_validator.py` accepts this spec. Resolution happens once, eagerly, the
next time this flow's pipeline graph is defined — see below.

### The general rule, and its one exception

Everywhere else in this framework, a secret resolves lazily: `resolve_secret_ref` is called
right where the plaintext value is needed (inside `column_crypto.py`'s per-column loop,
inside `_apply_source_zip_handling`, inside `zip_utils.py`), because every one of those call
sites runs on the normal Lakeflow pipeline driver process, which has a working `dbutils`
gateway.

`target_type: "sink"`/`"external_sink"` with `sink_config.format: "pgp_zip"`
(`archive/pgp_zip_sink.py`) is the one place this breaks down, for a genuine architectural
reason: it's a custom **Python Streaming Data Source Sink** (`pyspark.sql.datasource`,
Spark 4.0 / DBR 15.4+). Per that API's contract, `DataSourceStreamWriter.write()` runs on
executors (one call per partition, once per micro-batch); `commit()`/`abort()` are
documented as running "on the driver" — but confirmed live, that is a **separate, dedicated
"python streaming data source runtime" worker process**
(`pyspark/sql/worker/python_streaming_sink_runner.py`), not the same process as the main
pipeline driver notebook that owns a working `dbutils` gateway.

**A first version resolved the ZIP passphrase and PGP keys lazily inside `commit()`** — the
same pattern used everywhere else, and the one the API docs' "runs on the driver" language
suggests should just work. It failed every time, live, with:

```
Unable to resolve Unity Catalog secret '...': [Errno 13] Permission denied: '/databricks/spark/./bin/spark-submit'
```

— `DBUtils(spark)` trying and failing to spawn a gateway subprocess in that restricted
runtime. This is the same class of restriction Databricks documents for calling `dbutils`
from inside a UDF, and the fix is the one Databricks itself recommends for that class of
problem: **resolve every secret on the driver, in a context where `dbutils` actually works,
and pass the already-resolved plaintext value through as a plain argument** — never call
`resolve_secret_ref`/`dbutils` again once execution has moved into `write()`/`commit()`/
`abort()`.

### Where the eager resolution actually happens

`engine/sink_registration.py::_resolve_secret_into_options` does the resolving, at the point
`dlt.create_sink(name=..., format=..., options=...)` is being built — genuine
graph-definition time, on the normal pipeline driver process:

```python
def _resolve_secret_into_options(options, prefix, secret_ref):
    options[f"{prefix}_secret_value"] = resolve_secret_ref(SparkSession.getActiveSession(), secret_ref)
```

Called for every secret a `pgp_zip` sink might reference:

| `sink_config` field | Resolved into option key | Used for |
|---|---|---|
| `post_export_archive.secret` | `zip_secret_value` | Optional AES-256 password on the ZIP itself. |
| `post_export_archive.pgp_encryption.recipient_public_key_secret` | `pgp_recipient_secret_value` | Required when `pgp_encryption.enabled` — the recipient's public key. |
| `post_export_archive.pgp_encryption.sign_with_private_key_secret` | `pgp_sign_secret_value` | Optional — signs before encrypting when present. |

The resulting `options` dict is a plain `str -> str` mapping (all `DataSource` options are),
handed to `dlt.create_sink(...)` and, from there, to every `streamWriter()`/`write()`/
`commit()` call `PgpZipDataSource` makes for the life of the pipeline. `archive/pgp_zip_sink.py`
never imports `resolve_secret_ref` or touches `dbutils` at all — every `*_secret_value` it
reads out of `self.options` is already a resolved plaintext string by the time it gets there.

Every option key that carries a resolved value is still named `..._secret_value` (not, say,
`..._value`) **on purpose**: Spark's own credential-redaction machinery
(`spark.redaction.regex`, matching the keyword `secret`) keys off substring matching, so
keeping `secret` in the option *name* means it still gets redacted in query-plan/event-log
diagnostics, even though the value itself is no longer resolved through a `secret(...)` SQL
call (§2) — the redaction behavior is preserved deliberately, only the resolution mechanism
changed.

**`sink_config.kafka_secret_options` resolves eagerly too, for a related but distinct
reason** — not the restricted-worker-process issue above, but a structural one:
`dlt.create_sink(options=...)` itself only ever accepts a flat, already-built `str -> str`
dict at the point it's called. There is no lazy hook anywhere in a native Lakeflow sink's
options for a value to be resolved later, unlike a `@dlt.table`/`@dlt.view` closure body,
which Lakeflow re-invokes at execution time. So `_build_sink_options` resolves
`kafka_secret_options` the same way, at the same graph-definition-time point, purely because
that's the only point at which sink options can be built at all — prefer
`kafka_options["databricks.serviceCredential"]` (a Unity Catalog service-credential
*reference*, injected transparently by Databricks — see
[Configure a sink for a Lakeflow Declarative Pipeline](https://learn.microsoft.com/en-us/azure/databricks/ldp/ldp-sinks))
wherever possible, and reserve `kafka_secret_options` for a connector option whose literal
value must genuinely embed a resolved secret (e.g. `kafka.sasl.jaas.config`) with no
service-credential alternative.

**The rule for future contributors:** never add a call to `resolve_secret_ref`, `dbutils`,
or anything that constructs a `DBUtils` gateway inside `archive/pgp_zip_sink.py`'s
`write()`, `commit()`, or `abort()`. If a `pgp_zip` sink needs a new secret-backed option,
resolve it in `engine/sink_registration.py::_build_sink_options` (or a helper it calls) and
thread the plaintext through `options`, exactly like the three rows in the table above.

### What `commit()` actually does with the resolved values

For completeness — this is where the resolved secrets get used, not where they get
resolved. Per micro-batch: `write()` (on each executor, per partition) stages that
partition's rows as newline-delimited JSON to a uniquely-named file under `path` (the
staging directory) and returns the file's path in its `WriterCommitMessage`; `write()` never
touches a secret at all. `commit()` (on the driver, in the restricted worker process
described above) collects the paths from the messages it was actually handed — never by
re-deriving or globbing the staging directory, so it only ever archives exactly this
micro-batch's own output, never a stale file left behind by a prior aborted batch — moves
them into a batch-scoped directory, and calls `archive/zip_utils.py::compress_and_encrypt_sink`
with `passphrase=self._zip_secret_value` (the **already-resolved** value; that function
accepts either a `{secret_catalog, secret_schema, secret_key}` triple to resolve itself
*or* an already-resolved `passphrase` string — this call site always uses the latter, for
exactly the reason above). If `pgp_enabled`, the finished ZIP's bytes are then run through
`crypto/pgp.py::pgp_encrypt` with `self._pgp_recipient_key_armored`/
`self._pgp_sign_key_armored` (again, already-resolved) and the plaintext-intermediate ZIP is
deleted once its encrypted replacement is written — the unencrypted file never persists at
`output_zip_path`.

---

## 7. Provisioning and configuration

Nothing in `onboarding_templates/pipeline_onboarding_template.json` or `test_specs/*.json`
provisions a secret itself — every `secret_catalog`/`secret_schema`/`secret_key` reference
names a Unity Catalog secret that must already exist, with `READ SECRET` granted to
whatever identity runs the pipeline, before that flow is onboarded. Creating a UC secret
(the `CREATE SECRET`/grant statements, and the ACL model) is a manual, admin-audited step
external to this framework — see
[the Unity Catalog secrets documentation](https://docs.databricks.com/aws/en/security/secrets/unity-catalog-secrets)
for exact syntax and grant options. Provisioning key material is deliberately never
automated by an onboarding spec.

Recap of the one hard technical constraint on the *value* itself (§3): AES key material for
`encrypted_columns`/`decrypted_columns` must be exactly 16, 24, or 32 raw bytes. ZIP
passphrases (`source_zip_handling.pre_extraction_decryption.secret_passphrase`,
`post_export_archive.secret`) have no such
constraint — `pyzipper` derives its own key from the passphrase via its own KDF, so any
string works. PGP key secrets store the full ASCII-armored key block (public or private) as
the secret's value, exactly as `gpg --export --armor` / `gpg --export-secret-keys --armor`
would produce it.

---

## 8. Error handling

| Exception | Raised when |
|---|---|
| `SecretResolutionError` | A `secret_catalog`/`secret_schema`/`secret_key` triple is malformed, doesn't exist, or the caller lacks `READ SECRET`. |
| `CryptoError` | A missing/malformed `encrypted_columns`/`decrypted_columns` config entry; an unknown column; an unsupported `mode`; a `cast_to_type` that disagrees with a tagged `original_data_type` (§4, not yet reachable in production); PGPy unavailable, an unparseable key, a wrong/missing passphrase, or a failed encrypt/decrypt/sign/verify call. |
| `ArchiveError` | `pyzipper` unavailable; a ZIP is corrupt or its passphrase is wrong; `pgp_zip` sink options are missing `path`/`output_zip_path`, or `pgp_enabled: true` with no `pgp_recipient_secret_value`. |
| `FrameworkConfigError` | `sink_config` is missing/malformed for `target_type` `sink`/`external_sink` (defense-in-depth — `onboarding/spec_validator.py` already rejects most of these at onboarding time; this is the same check re-run against whatever a control-table row actually contains, in case it was written some other way). |

---

## Reference

* [Unity Catalog secrets](https://docs.databricks.com/aws/en/security/secrets/unity-catalog-secrets)
* [`aes_encrypt`](https://docs.databricks.com/en/sql/language-manual/functions/aes_encrypt.html) /
  [`aes_decrypt`](https://docs.databricks.com/en/sql/language-manual/functions/aes_decrypt.html)
* [PGPy](https://pypi.org/project/PGPy/) (OpenPGP, pure Python)
* [Create a PySpark data source for streaming read and write](https://learn.microsoft.com/en-us/azure/databricks/pyspark/datasources) —
  the `DataSourceStreamWriter` API `archive/pgp_zip_sink.py` implements.
* [Configure a sink for a Lakeflow Declarative Pipeline](https://learn.microsoft.com/en-us/azure/databricks/ldp/ldp-sinks) /
  [sink limitations](https://learn.microsoft.com/en-us/azure/databricks/ldp/concepts/sinks#limitations)
* [Redacted possible secret access key as part of column value](https://community.databricks.com/t5/data-engineering/redacted-possible-secret-access-key-as-part-of-column-value/td-p/55086) —
  the community-confirmed redaction bug behind §2.
