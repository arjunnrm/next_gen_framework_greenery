# Common Pitfalls — Read Before Editing This Code

Every entry below is a **real bug that actually shipped in this codebase**, was caught (often
only by a later, more exhaustive test suite), and fixed — documented here so it doesn't get
silently reintroduced. Each one originally looked correct at review time; that's exactly why
it's worth internalizing the underlying mechanism, not just the specific line that got fixed.
File paths are relative to `src/NextGen_Metadata_Framework/lakeflow_framework/` unless noted.

---

### 1. Never let a literal `secret(...)` SQL substring reach a Lakeflow graph-definition query

**Wrong:** `F.expr("aes_encrypt(CAST(col AS STRING), secret('scope', 'key'), 'MODE')")`.

**What happens:** Databricks' platform-wide credential-redaction machinery
(`spark.redaction.regex`, which matches the keyword `secret`) treats any expression whose
*query text* contains a `secret(...)` call as credential-bearing and redacts/mutates it as
part of Lakeflow's own per-dataset metrics/observability pipeline. This **silently corrupts
the result** — every encrypted value in the real, live-confirmed case decoded back to a
byte-length-inflated string riddled with `U+FFFD` replacement characters (a lossy UTF-8
decode/re-encode signature). It is invisible to a test that only checks "not plaintext"; it
takes an actual round-trip assertion to catch. Isolated non-Lakeflow Structured Streaming
writes using identical SQL text never reproduce it — this is specific to Lakeflow's own
logging layer.

**Fix, and the rule going forward:** resolve the secret's plaintext value first via
`crypto/secrets.py::resolve_secret_ref` / `resolve_secret_value`, then pass it into a PySpark
**column function** as a literal `F.lit(resolved_value)` — never as interpolated SQL text.
Unity Catalog secrets have no SQL-callable resolution function at all (`dbutils.secrets.get()`
is the only path), so this exact bug can't recur for a UC secret — but the underlying lesson
(never splice a `secret(`-shaped substring into a Lakeflow-materialized expression) applies to
any future SQL-text-building code too. See `crypto/column_crypto.py` and `crypto/secrets.py`
module docstrings, and `docs/16_encryption_and_secrets.md` §2.

---

### 2. Unity Catalog secrets are 3-level (`secret_catalog`/`secret_schema`/`secret_key`) — never a classic scope

Every secret reference in this framework is `{secret_catalog, secret_schema, secret_key}`,
resolved via `dbutils.secrets.get(catalog=, schema=, key=)`
(`crypto/secrets.py::resolve_secret_value`). Do not introduce (or accept from a user) a
`secret_scope`/`secret_key` pair (the classic workspace-scope shape) anywhere — it's a
different, older system that cannot address a Unity Catalog secret, and this framework
migrated away from it deliberately (v1 → v2). Requires Databricks Runtime 17.3 LTS+ /
serverless environment version 4+.

---

### 3. AES-GCM has a random IV per call — never put an encrypted column in a CDC comparison set

`GCM` (the default and recommended AES mode) re-encrypts identical plaintext to *different*
ciphertext on every single call. If an encrypted column ends up in
`target_config.columns_to_check` (or isn't excluded via `columns_to_exclude`) for an SCD1/
SCD2/SCD3 flow, the change-comparison will see a "change" on **every row, every single run**,
even when nothing actually changed — silently blowing up SCD2 history size and defeating
SCD1/SCD3's whole purpose. Always keep encrypted columns out of `columns_to_check`, or list
them in `columns_to_exclude`. See `docs/16_encryption_and_secrets.md` §3 and
`docs/10_test_pipeline_2_streaming_cdc.md`.

### 3a. AES key material must be exactly 16, 24, or 32 raw bytes

`aes_encrypt`/`aes_decrypt` reject anything else with `INVALID_PARAMETER_VALUE.AES_KEY_LENGTH`.
A base64-encoded 32-byte value is 44 *characters* as a string and fails this check;
`openssl rand -hex 16` (32 hex characters = 32 bytes when stored as a string) is a valid
AES-256 key. This is about the resolved *value*, not where/how the secret is stored.

---

### 4. Auto TTL is not a generic `delta.*` table property

**Wrong:** `properties["delta.autoTTL.duration"] = target_config["auto_ttl_duration"]` folded
into the same dict `build_table_properties` returns.

**What happens:** that property name doesn't exist. Delta silently ignores an unrecognized
custom key — **no error, ever** — so every flow that ever configured this got zero TTL
behavior, from the feature's very first commit, with nothing to reveal the failure.
[Auto TTL](https://docs.databricks.com/aws/en/tables/operations/auto-ttl)'s real properties
are `autottl.timestampColumn`/`autottl.expireInDays`, and for a Lakeflow streaming table it can
**only** be set via a dedicated `auto_ttl={"timestamp_column": ..., "expire_in_days": ...}`
keyword argument passed directly to the table-creating `@dlt.table`/
`dlt.create_streaming_table` call — `ALTER TABLE ... DELETE ROWS` is explicitly unsupported
for streaming tables.

**Fix / the rule:** build `auto_ttl` as its **own** decorator kwarg
(`storage/table_properties.py::build_auto_ttl_kwarg`, threaded through as `table_kwargs["auto_ttl"]`
alongside `partition_cols`/`cluster_by` — see `dq/quarantine.py::register_main_and_quarantine_tables`),
never merged into the generic `table_properties` dict. If you add a new Delta/Lakeflow
setting that isn't a plain `TBLPROPERTIES`-style key, check whether it needs its own decorator
kwarg the same way before assuming `build_table_properties` is the right place for it. See
`docs/21_auto_ttl_row_expiration.md`.

---

### 5. Unity Catalog Volumes' FUSE mount does not support seek-on-write

**Wrong:** `pyzipper.AESZipFile(output_zip_path, "w", ...)` writing directly to a `/Volumes/...`
path.

**What happens:** the ZIP container format requires seeking back to patch local file headers
and write the central directory once all members are written. Volumes' FUSE mount only
supports sequential writes, so this raises `OSError: [Errno 5] Input/output error` on
`close()`.

**Fix:** build the archive in an in-memory `io.BytesIO()` buffer (fully seekable), then write
the finished bytes out to the Volume path with one plain, sequential `open(path, "wb").write(...)`.
See `archive/zip_utils.py::compress_and_encrypt_sink`.

---

### 6. `fnmatch.fnmatch` is case-insensitive on Windows, case-sensitive on Linux — use `fnmatchcase`

Any pattern-matching against filenames landed by a real source (ZIP archive selection,
file-pattern matching) must use `fnmatch.fnmatchcase`, not `fnmatch.fnmatch` — `fnmatch`
case-normalizes per the **local OS**, which is case-insensitive on a Windows dev machine but
case-sensitive on the Linux compute Databricks actually runs on. Code developed and "tested"
on Windows can pass locally while silently failing to match on real compute. See
`ingestion/readers.py::_apply_source_zip_handling`.

---

### 7. Delta collects data-skipping stats for only the first ~32 columns — column *position* matters

`delta.dataSkippingNumIndexedCols` defaults to 32 — a column-**position** limit, not a
configurable whitelist. Appending a framework-computed column via the natural, simplest
pattern (`df.withColumn("new_col", ...)`) always places it **last** in the schema, which
silently drops it out of stats coverage the moment a table has more than ~32 business columns
ahead of it — exactly the columns (primary keys, clustering keys, the hash columns
reconciliation joins on) that most need that coverage at scale. Before any new
framework-generated column becomes part of a materialized table's schema, check whether it
needs to be front-loaded via `storage/column_ordering.py::reorder_columns_for_delta_stats`
(already called once, centrally, in `dq/quarantine.py::register_main_and_quarantine_tables` —
don't reimplement this per-caller).

---

### 8. `dbutils` does not work inside a Python Streaming Data Source's `write()`/`commit()`/`abort()`

**Wrong:** resolving a secret lazily, inside a custom sink's `commit()` (which Databricks'
own docs describe as running "on the driver" — a reasonable-looking assumption).

**What happens:** `commit()`/`abort()` for a Python Streaming Data Source actually execute in
a **separate, dedicated** "python streaming data source runtime" worker process
(`pyspark/sql/worker/python_streaming_sink_runner.py`), not the main pipeline driver notebook
process that owns a working `dbutils` gateway. Confirmed live: every attempt raises
`Unable to resolve Unity Catalog secret '...': [Errno 13] Permission denied:
'/databricks/spark/./bin/spark-submit'`. This is the same class of restriction Databricks
documents for calling `dbutils` from inside a UDF.

**Fix / the rule:** resolve **every** secret a custom sink needs *before* that sink ever
runs — in the normal pipeline graph-definition process (`engine/sink_registration.py::
_resolve_secret_into_options`, `_build_sink_options`) — and pass the already-resolved
plaintext values through as plain string options. Neither `write()` nor `commit()`/`abort()`
in `archive/pgp_zip_sink.py` ever calls `resolve_secret_ref`/`dbutils` itself. If you add a
new sink format or a new custom Data Source anywhere in this framework, resolve its secrets
the same way, eagerly, at the call site that still runs in the normal driver process.

---

### 9. A `@dlt.view` is not a durable, queryable object after the pipeline update finishes

A `@dlt.view` only exists for the lifetime of one Lakeflow graph execution — confirmed
empirically, it never appears in `SHOW TABLES` for any schema once the update completes.
Anything meant to be queried **after** the pipeline runs (a reporting dataset, an export
source for a later job task) must be a real `@dlt.table`, never a `@dlt.view`, even if a
view would otherwise be the "right" abstraction (no extra storage, purely derived). This bit
the SCD2 reporting view twice in sequence: first a qualified `@dlt.view` name raised
`AnalysisException: View with multipart name '...' is not supported` (views can't be
qualified like tables can — see next entry), and once changed to an unqualified view, it
raised `TABLE_OR_VIEW_NOT_FOUND` the first time anything tried to query it after the pipeline
finished. See `cdc/scd.py::register_scd2_reporting_view`.

---

### 10. A bare `name=` on `@dlt.table`/`@dlt.view` resolves against the *pipeline's* default schema, not the flow's

Lakeflow resolves every unqualified `name=` against the pipeline resource's own default
catalog/schema (`resources/*.yml`), **not** against a flow's own `target_catalog`/
`target_schema` control-table columns. Passing a bare `target_table` silently lands every
flow in the pipeline's default schema instead of its configured target — only surfaces once a
pipeline has flows spanning more than one schema and something tries to read a table by its
*intended* qualified name. **Always** build the name via
`storage/table_properties.py::qualified_table_name(catalog, schema, table)` for every table/
view a flow publishes as a final, externally-queryable dataset (main table, quarantine table,
SCD reporting views, SCD3's internal history table). A `@dlt.view`'s `name=` cannot be
qualified at all (see entry 9) — that's a separate, additional constraint, not a substitute
for qualifying every real table.

---

### 11. Registering the CDC target under the same name a `@dlt.table` already claimed raises "Cannot redefine dataset"

When a flow's `cdc_load_strategy` is anything other than `APPEND`/`TRUNCATE_AND_LOAD`, the CDC
dispatcher (`cdc/dispatcher.py` → `cdc/scd.py`/`cdc/snapshot.py`) goes on to publish the
*actual* target table under the flow's `target_table` name via `dlt.apply_changes`/
`apply_changes_from_snapshot`. Unconditionally also registering a plain `@dlt.table` under
that same qualified name for the "clean" (post-quarantine) side raises `Cannot redefine
dataset` the first time such a flow is actually deployed. The fix baked into
`dq/quarantine.py::register_main_and_quarantine_tables` is a `needs_cdc_dispatch` gate: when
`True`, the clean side is registered as an **internal** `@dlt.view` (`_<target_table>_clean`)
instead, freeing the real name for the CDC dispatcher to own. If you add a new CDC-dispatched
strategy or a new target-registration path, preserve this same gate — don't materialize a
table under a name a CDC strategy will also try to publish.

---

### 12. `${param}` substitution already quotes string values — don't quote them again in your SQL

`transformation/parameters.py::substitute_dynamic_parameters` renders a string parameter value
as a single-quoted SQL literal itself. Writing `WHERE country = '${filter_country}'` in
`transformation_sql`/`filter_condition`/`transform_sql` becomes `WHERE country = ''US''` — a
`ParseException` — because the substitution already supplied the quotes. Write
`WHERE country = ${filter_country}` instead. This exact mistake shipped in this repo's own
fixture specs until a real pipeline run caught it.

---

### 13. Databricks Connect / Spark Connect is lazy — `try/except` around a DataFrame transform doesn't catch an analysis error until an action runs

Building a DataFrame plan (`.withColumn(...)`, `.select(...)`, etc.) on Spark Connect /
serverless is purely client-side plan construction — it does **not** round-trip to the server
and therefore **cannot raise** for a reference to a genuinely-absent column. The error
(`AnalysisException`) only surfaces once something forces eager analysis — `.schema`,
`.collect()`, `.count()`, or similar. A `try/except` wrapped tightly around the `withColumn`
call itself is dead code; the real exception erupts later, uncaught, from whatever unrelated
line happens to trigger analysis. Concretely: `asn1/decoder.py::
_materialize_hidden_metadata_column` deliberately forces `candidate.schema` access **inside**
its own `try` block specifically so its fallback can actually catch a genuinely-missing
`_metadata` column — accessing `.schema` anywhere later (as the original, broken version did)
made the surrounding `try/except` useless. When wrapping a DataFrame transform for
graceful degradation, force resolution (`.schema` is cheapest) *inside* the same `try` you
want to actually catch with.

A related, sharper case: `onboarding/spec_validator.py::_validate_sql_syntax`'s `EXPLAIN`-based
SQL-syntax check found that on this workspace's Spark Connect/serverless environment,
`EXPLAIN` **never raises any exception at all** for a query-planning problem (not
`ParseException`, not `AnalysisException`) — it always returns a normal DataFrame, and a
failed plan's text simply starts with a literal `"Error occurred during query planning: "`
line instead of `"== Physical Plan =="`. Don't assume a `spark.sql(...)`/`EXPLAIN` call raises
on a bad query in this environment — collect the result and inspect its text.

---

### 14. `DataFrame.cache()`/`.persist()` is rejected outright on serverless compute

Every Lakeflow Job in this framework runs on serverless compute, which raises
`[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute` for
both `.cache()` and `.persist()` — confirmed empirically against this project's own `dev`
profile. Do not add a `.cache()`/`.persist()` call anywhere expecting it to avoid a redundant
re-scan (e.g. reconciliation re-reading the same source DataFrame for several targets) — it
will fail outright, not just skip an optimization. `reconciliation/matcher.py` accepts the
resulting redundant re-reads deliberately (relying on Delta's own file skipping to bound the
cost) rather than caching; if you need to avoid a genuine repeated-scan cost on serverless,
materialize to a temporary Delta table instead.

---

### 15. PGPy: `bytes(PGPMessage)` is binary, `str(PGPMessage)` is ASCII-armored — they are not interchangeable

`pgp_encrypt` must return `str(encrypted_message).encode("utf-8")`, **not**
`bytes(encrypted_message)`. `bytes(PGPMessage)` serializes to the compact **binary** OpenPGP
packet format (no `-----BEGIN PGP MESSAGE-----` armor); `str(PGPMessage)` produces the
ASCII-armored text this function's docstring promises. This framework's own `pgp_decrypt`
(via PGPy's `from_blob`) happily parses either, so a round-trip test using only this
codebase's own encrypt/decrypt pair passes regardless of which one you pick — but a real
external recipient (a standard `gpg` CLI, or any tool assuming armored text) cannot reliably
decrypt binary output. See `crypto/pgp.py::pgp_encrypt`.

Separately: `pgp_decrypt`'s payload for a binary (`file=True`) message comes back from PGPy as
a `bytearray`, **not** `bytes` — a plain `isinstance(payload, bytes)` check misses it and
falls into the wrong branch (`AttributeError: 'bytearray' object has no attribute 'encode'`).
Since `pgp_encrypt` in this framework always builds binary messages
(`PGPMessage.new(data, file=True)`), this bug made every real PGP-then-ZIP decrypt path in
this framework fail on every real invocation until fixed. Check `isinstance(payload, (bytes,
bytearray))` and coerce with `bytes(payload)`.

---

### 16. `_metadata.file_metadata` does not exist — extended file headers live on a separate `_object_metadata` pseudo-column

Auto Loader's hidden `_metadata` struct has exactly six fields
(`file_path`/`file_name`/`file_size`/`file_modification_time`/`file_block_start`/
`file_block_length`) — never a `file_metadata` field. Extended/custom-header properties
(MIME type, ETag, user/system metadata, tags) live on a **separate** hidden pseudo-column,
`_object_metadata`, not nested under `_metadata` at all, and require Databricks Runtime 18.2+.
A wrong field name silently swallowed by a defensive `try/except` (see entry 13 — this is
exactly that pattern) produces a column that is unconditionally `NULL` for every row, on every
source, with nothing to reveal the mistake. See
`ingestion/technical_metadata.py::attach_technical_metadata`.

---

### 17. A watermarked event-time column ingested from JSON/CSV routinely arrives as `STRING`, not `TIMESTAMP`

`DataFrame.withWatermark(col, threshold)` requires an actual `TimestampType` column, but
Auto Loader has no native timestamp type to infer from free-text JSON/CSV — an ISO-8601
event-time field commonly lands as `STRING`. Calling `withWatermark` directly on it raises
`EVENT_TIME_IS_NOT_ON_TIMESTAMP_TYPE`. Always cast first:
`F.col(event_time_column).cast("timestamp")` (a no-op when it's already a timestamp) — see
`transformation/inputs.py::register_transformation_inputs`.

---

### 18. Reconciliation: never classify per *joined row* when either side can have duplicate keys

A reconciliation target is frequently an append-only CDC/Zerobus-style bus and may
legitimately hold more than one historical row for the same logical key. Classifying
match/drift/miss **per joined row** (rather than per distinct key) lets the same source
record appear as both matched (via one target-side row) and unmatched (via another,
stale, target-side row for the same key) simultaneously — this made a prior version of the
self-healing append never converge, re-appending a fresh "correction" duplicate on every
subsequent run. The fix: after the hash-key join, collapse every group sharing the same
`__framework_hash_key` to **one** representative outcome via `F.max_by` ordered by an explicit
`MATCHED > VALUE_DRIFT > MISSING_*` priority, in a single groupBy/aggregate pass. See
`reconciliation/matcher.py::match_reconciliation_target`'s module/function docstrings before
changing anything about how matches are classified.

---

### 19. Reconciliation: count the target's pre-run size *before* appending corrections into it — not after

`appender.py::run_target_reconciliation`'s target dataset is frequently the **same table**
its own `append_target_table` writes corrections into. Calling `.count()` on the prepared
target DataFrame only *after* `append_missing_records` has already written this run's own
corrective rows into that table silently counts this run's own just-appended rows as if they
were present when the run started — corrupting the "how many records were in the target
before this run" metric (`target_record_count`). Capture that count as a plain Python `int`
**immediately after preparing** the target DataFrame, before any write happens — never defer
it to the end of the function on a lazy DataFrame reference. See
`reconciliation/appender.py::run_target_reconciliation`'s inline comment at the
`target_record_count = prepared_target_df.count()` line.

---

### 20. ASN.1: compile the schema once per partition, never once per row

A plain row UDF (`F.udf`) calling `asn1tools.compile_files(...)` *inside* the per-row decode
function recompiles the full ASN.1 module from scratch for every single row — Spark has no
way to know that call is expensive and reusable. Use `DataFrame.mapInPandas` instead,
compiling once **before** iterating a partition's batches (`asn1/decoder.py::
make_partition_decoder`) — this cuts compilation from O(rows) to O(partitions). If you ever
add another binary-format decoder to this framework, follow the same shape: compile/prepare
expensive, reusable state once per partition inside the `mapInPandas` generator function, not
inside a per-row UDF.

Two related ASN.1-specific facts worth knowing before touching a `.asn` spec: identifiers are
camelCase with no underscores (per X.680 — expect `callDurationSeconds`, not
`call_duration_seconds`), and `BIT STRING` decodes from `asn1tools` as a bare
`(bytes_or_bytearray, bit_length)` tuple that must be reshaped into a named
`{"bytes": ..., "bit_length": ...}` struct to match its declared Spark output type — see
`asn1/decoder.py::_normalize_decoded_value`.

---

### 21. Governance tag DDL and CDC change-count capture must run *after* the pipeline update, never inside it

`ALTER TABLE ... SET TAGS` (`governance/tags.py`) and any query against
`table_changes(...)`/`DESCRIBE HISTORY` (`cdc/change_metrics.py`) both require a table that
has already been materialized/committed — impossible from inside the pipeline's own
graph-*definition* code, which only ever builds lazy query plans that Lakeflow executes and
commits *after* graph-definition finishes. Both are deliberately implemented as
post-deployment functions (`control_plane/post_deployment.py::apply_all_governance_tags` /
`capture_all_scd_change_counts`), called from a separate downstream job task
(`notebooks/04_governance/04_apply_governance_and_egress.py`), never wired into
`notebooks/03_engine/03_lakeflow_declarative_pipeline.py` itself. If a future feature needs to
read a flow's own materialized state (a row count, a commit version, a table property),
default to assuming it needs the same post-deployment treatment rather than trying to fit it
into graph-definition-time code.

---

### 22. Sink egress must be a genuine `dlt.create_sink`/`@dlt.append_flow`, never a separate batch write

A previous version of `external_sink` egress ran as a *post-deployment* plain-Spark step
(`spark.read.table(...).write.format(...).save(...)`, in a now-removed
`control_plane/post_deployment.py::run_external_sink_exports`) — racing an ordinary batch
read/write against whatever the pipeline had most recently materialized, entirely outside the
pipeline's own DAG. This is exactly the anti-pattern the project's own requirement forbids.
Every `sink`/`external_sink` export must be registered inside the pipeline graph at
graph-definition time via `engine/sink_registration.py`, executed as part of the same update
that produces the data it reads. If you're asked to add a new export destination, it is a new
`sink_config.format` (§7 in `SKILL.md`), not a new post-deployment job step.
