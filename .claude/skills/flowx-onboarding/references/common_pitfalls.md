# Common Pitfalls — Read Before Editing This Code

Every entry below is a **real bug that actually shipped in this codebase**, was caught (often
only by a later, more exhaustive test suite), and fixed — documented here so it doesn't get
silently reintroduced. Each one originally looked correct at review time; that's exactly why
it's worth internalizing the underlying mechanism, not just the specific line that got fixed.
File paths are relative to `src/flowx/lakeflow_framework/` unless noted.

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
catalog/schema (`resources/**/*.yml`), **not** against a flow's own `target_catalog`/
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

---

### 23. "Never put an eager action inside a dataset query definition" is WRONG as stated — the real prohibition is eager-on-a-**streaming** plan

This rule has circulated in this repo (and in this project's memory notes) as an unqualified
"Rule 2: never put an eager action inside a `@dlt.table`/`@dlt.view` closure". Stated that way it
is false, and it is falsified by code this repo **ships today**:
`dq/quarantine.py::register_main_and_quarantine_tables`'s `_quarantine_table` closure runs

```python
agg_row = upstream.agg(F.count(...), F.sum(...)).collect()[0]
```

*inside* a live `@dlt.table` closure, on the **batch** branch, under a comment that says in so many
words that this closure body runs at Lakeflow's graph-**execution** time (see the batch/streaming
branch around `dq/quarantine.py:519-540`). Only the streaming branch is guarded, and the comment
there gives the real reason: a streaming DataFrame cannot be eagerly aggregated or collected.

**The three real prohibitions**, which is what any new guard (or AST check) must encode:

1. **Eager action on a STREAMING plan** — `.count()`, `.collect()`, `.isEmpty()`,
   `.limit(1).take(1)` against a streaming DataFrame. Raises outright.
2. **Self-read** — a dataset that reads itself, directly or through
   `spark.read.table("<its own qualified name>")` (see entry 24). Lakeflow aborts graph
   construction: `Graph is not topologically sorted. There is a cycle between <t> and <t>`.
3. **Side-effecting writes** — any `.write`/`saveAsTable`/DDL from inside a query definition. The
   only legal graph terminal is `dlt.create_sink` + `@dlt.append_flow` (or
   `dlt.foreach_batch_sink`).

The original bug that produced the over-broad rule is still a real bug, but for a *different*
reason: v1.3.0's E09 empty-source guard called `source_df.isEmpty()` in a closure and could not
distinguish "the source is genuinely empty" from "the upstream this same update produces has not
materialized yet". That is a **freshness** problem, not an eagerness problem. Getting this
distinction right is what licenses the v1.5.0 in-pipeline reconciliation metrics MV; writing the
guard as the blanket rule would have failed against the framework's own shipped code.

---

### 24. A fully-qualified three-part name is a **sibling reference**, not an external read

`spark.read.table("cat.sch.tbl")` inside a pipeline is **not** an escape hatch from the Lakeflow
graph. If `cat.sch.tbl` happens to be a dataset published by the *same* pipeline, Lakeflow creates a
real dependency edge exactly as `dlt.read` would — this is precisely how the self-read cycle above
was originally triggered (E09's `_read_existing_target_or_none` used a plain `spark.read.table` on
the flow's own qualified main table and Lakeflow still aborted with the topological-sort cycle; the
comment survives at `dq/quarantine.py:403-412`).

Two consequences that are easy to get wrong in opposite directions:

- **Any "read the far side outside the graph" design does not work.** Reaching for a plain Spark
  read to dodge a graph edge fails; you get the edge anyway, or a cycle.
- **The framework relies on this being true.** Because
  `storage/table_properties.py::qualified_table_name` makes a published dataset's graph identity
  byte-identical to the plain string a spec's `source_config.table` holds, a reconciliation or
  transformation side pointed at a table this same group publishes is resolved by
  `engine/source_plane.py::bind` to an `in_graph_sibling` binding — a genuine producer→consumer
  edge, topologically ordered and same-update fresh, with **no second physical read**. Registering a
  source-plane node for such a locator would be a *second* read of something the graph already
  produces, which is why `bind` refuses to.

Also do not point a source at a published table's **backing storage path** to "avoid" the edge.
That reads a Delta location the pipeline is concurrently committing to, outside the graph's
ordering, and is forbidden.

---

### 25. A `@dlt.view` is declared once but **read** once per consumer — it is not "read once"

Entry 9 covers a view's *lifetime*. This is the separate, sharper problem: Lakeflow **inlines** a
view into every consumer's plan. Two consumers of one `@dlt.view` over an Auto Loader path open
**two independent `cloudFiles` streams over that path, sharing one `cloudFiles.schemaLocation`** —
observed live on TC-DQ-004 (a streaming Auto Loader source with two quarantine rules). "Declared
once" is not "read once", and no amount of view reuse makes it so.

Only **materialization** makes the read-once requirement (R2) literally true. The v1.5.0 rules,
encoded in `engine/source_plane.py`:

- **MATERIALIZED:** every L0 shared source-plane node; every L3/L4 reconciliation dataset; a staged
  view whose flow has quarantine rules or a sink target.
- **VIEWED:** a single-consumer staged view; every transformation input view (a thin overlay over an
  already-materialized binding); the L5 `@dlt.append_flow`.

Two corollaries worth memorizing:

- **`read_mode` is not part of a read's identity.** One materialized *streaming table* legally
  serves `dlt.read_stream` **and** `dlt.read` consumers in the same update. A `@dlt.view` can serve
  neither pair — reading a streaming view with batch `dlt.read()` raises `View <name> is a streaming
  view and must be referenced using readStream`. So a streaming/batch collision is **resolved** by
  collapsing to a streaming table, never split into two nodes, and never papered over with
  `pipelines.incompatibleViewCheck.enabled=false` (pipeline-wide, and it silences the check without
  making a streaming plan batch-readable — recorded as known-and-rejected in
  `docs/13_known_limitations_and_gotchas.md`).
- **Materialization is not free and the framework does not pretend it is.** A shared node is a full
  physical copy in UC storage, an extra DAG step, a checkpoint in the streaming case, and — the part
  usually missed — it **destroys predicate pushdown** of a consumer's `filter_condition` into the
  original source, which `reconciliation/matcher.py` explicitly relies on. That is why
  `source_plane.materialize` USED to default to `"auto"` (a node only at fanout ≥ 2), with
  `"never"` as an escape hatch for a huge, heavily-filtered table. **v1.7.3's Single-Read
  architectural mandate reverses that trade:** the default is now `"always"` — every external
  identity is materialized regardless of fanout — and `"never"` is prohibited (rejected at
  onboarding, and again at plan time for rows persisted before the mandate). `"auto"` is still
  accepted but resolves to `"always"`. The pushdown cost above is now knowingly paid.

---

### 26. A column added to a `CREATE TABLE IF NOT EXISTS` DDL never reaches an existing workspace

Every statement in `control_plane/ddl_definitions.py::get_all_control_table_ddls` is
`CREATE TABLE IF NOT EXISTS`. Against a control table that already exists that statement is a
**no-op** — it does not diff the column list, and it does not add the new column. So a column added
to a CREATE DDL reaches **new installations only**, and every workspace provisioned before that
change silently keeps the old schema.

**How it actually surfaced (v1.5.0, defect D4, verified live 2026-08-31):** `execution_mode` /
`publish_schema` / `dq_config_json` were added to `reconciliation_flow_spec`'s CREATE DDL and the
tests passed, because unit tests build the table from scratch. On the real `flowx` workspace the
table had **none** of the three, and pipeline-mode onboarding died at `MERGE` time with
`UNRESOLVED_COLUMN`.

**Fix, and the rule going forward:** a new control-table column goes in **two** places —

1. the table's `CREATE TABLE` DDL (so a fresh install gets it), and
2. `ddl_definitions.py::ADDITIVE_CONTROL_TABLE_COLUMNS` (bare table name → list of
   `(column_name, sql_type, comment)`), which
   `control_plane/schema_provisioner.py::ensure_control_table_columns` walks at the end of
   `ensure_control_schema_exists`, issuing `get_add_column_ddl(...)` for anything missing.

The migration is **strictly additive** by design — only `ALTER TABLE ... ADD COLUMNS`, never a drop
and never a retype. Existing rows get NULL, which is exactly what each column documents as its
default (`execution_mode` NULL means `"job"`, so an already-onboarded flow keeps its current
behaviour rather than silently switching hosts).

**The operational half, which is easy to miss:** `databricks bundle deploy` does **not** apply this
migration. Only *running* the `setup_control_tables` task
(`notebooks/01_setup/01_setup_control_tables.py`) does. A deploy followed straight by an onboarding
run on a pre-v1.5.0 workspace still fails with `UNRESOLVED_COLUMN`.

Note also that the brace convention flips between the two structures: the CREATE DDL f-strings must
double every literal brace as `{{ }}`, while `ADDITIVE_CONTROL_TABLE_COLUMNS` comments are plain
strings concatenated into SQL and must **not** be doubled.

---

### 27. `ADD COLUMNS IF NOT EXISTS` is a `PARSE_SYNTAX_ERROR` on Databricks SQL

`IF NOT EXISTS` is accepted on `CREATE TABLE` and on `ALTER TABLE ... ADD PARTITION`, so it reads as
if it should work on `ADD COLUMNS` too. It does not — Databricks SQL rejects
`ALTER TABLE t ADD COLUMNS IF NOT EXISTS (c STRING)` outright (verified live on DBR serverless,
2026-08-31). This is a **parse** failure, so it is not something a `try/except` around the statement
can be tuned around; the statement never runs at all.

**Idempotence is therefore caller-side**, and `schema_provisioner.py::ensure_control_table_columns`
implements it in two layers:

- **Skip what is present** — read `spark.table(qualified).columns`, casefold, and skip any column
  already there. This is the normal path.
- **Swallow the narrow race** — two `01_setup_control_tables` runs racing each other can both see
  the column absent. `_is_duplicate_column_race` matches only
  `FIELD_ALREADY_EXISTS` / `FIELDS_ALREADY_EXIST` / `COLUMN_ALREADY_EXISTS` /
  `COLUMN ALREADY EXISTS` (both the SQLSTATE-style condition names DBR raises and the plain
  sentence the Delta library raises). It deliberately does **not** substring-match the bare phrase
  `ALREADY EXISTS`: that also appears in unrelated failures, and swallowing one of those would turn
  a genuine provisioning error into a silent no-op — leaving the column absent and the next write
  failing with the very `UNRESOLVED_COLUMN` the migration exists to prevent.

Anything else still raises `FrameworkConfigError`. A missing *table* is only a warning, because
`ensure_control_schema_exists` has just run the CREATE statements and a table absent after that
means a permission problem its own error already reported more precisely.

---

### 28. Declaring a column in an upsert `StructType` is not enough — it must also be set in the `Row(...)` literal

`onboarding/metadata_upsert.py` describes each control-table row twice over: once as a module-level
`StructType` (`_RECONCILIATION_FLOW_SPEC_SCHEMA` and its siblings) and once as the `Row(...)`
literal the upsert actually constructs. **Adding a field to the schema alone compiles, passes every
schema-shape assertion, and writes NULL forever.**

**What actually shipped (v1.5.0, defect D1):** `upsert_reconciliation_flow_spec` declared
`two_tier_verification`, `execution_mode`, `publish_schema` and `dq_config_json` in the
`StructType` but never wrote them in the `Row(...)`. Every pipeline-mode flow was persisted with
`execution_mode` NULL, read back as `"job"` by
`control_plane/repository.py::load_active_group_metadata`, and filtered straight out of the DAG —
i.e. **the entire feature could never turn on**, with no error anywhere. The spec validated, the
onboarding job reported success, and the control-table row looked plausible.

**The rule:** when adding an attribute, grep for its name in `metadata_upsert.py` and confirm it
appears **at least twice** — in the schema *and* in the `Row(...)`. Keep an unset optional as
`None` (SQL NULL) rather than substituting a Python-side default, so the DDL-documented default
stays the single authority (`execution_mode` NULL ⇒ `"job"`, `two_tier_verification` NULL ⇒ true).

Same shape of bug, same check: a JSON key the validator parses but the upsert never persists is a
key the operator can set and the engine can never see.

---

### 29. A validation rule placed inside a function that early-returns is silently dead

A validator helper beginning with a guard such as `if not reconciliation_flows: return` is a
perfectly sensible shape — right up until an unrelated rule is added *inside* it. From then on the
rule fires only for specs that happen to satisfy the guard, with no signal at all: no error, no
warning, and a green test suite for as long as every fixture exercising the rule also satisfies the
guard.

**What actually shipped (v1.5.0, defect D2):** V-CYC-8
(`_validate_landing_side_effect_collisions`, which rejects two ingestion flows landing on one raw
path while disagreeing about `landing_retention_policy`/`source_zip_handling`) was invoked from
inside `_validate_reconciliation_pipeline_placement`, which returns early when a spec has **no
reconciliation flows**. V-CYC-8 is a pure *ingestion* rule — so it was dead for exactly the
ordinary Auto Loader spec it exists to protect. It is now called from `validate_spec` directly.

**The rule:** a validation rule belongs in `validate_spec`'s own top-level call sequence unless it
is genuinely scoped to the enclosing helper's subject. When adding a rule to an existing helper,
read that helper's first ten lines for a guard clause before assuming it runs.

---

### 30. A rule about a *graph* cycle must be gated on pipeline mode, or it breaks job-mode specs that always onboarded

The V-CYC append-loop rules (V-CYC-2, V-CYC-3 same-group, V-CYC-5, and the `target_configs`
self-/cross-append checks) describe a **Lakeflow DAG cycle**: a reconciliation flow appending into a
table the same pipeline update also produces. That cycle is real and fatal under
`execution_mode: "pipeline"` / `"pipeline_audit_only"`. Under `"job"` it **does not exist** — the
standalone engine in `notebooks/05_reconciliation/` runs *after* the update has finished, so there
is no graph left to be cyclic.

**What actually shipped (v1.5.0, defect D3):** these rules fired unconditionally, as hard errors.
That was a genuine **backward-compatibility break**: the shipped, pre-v1.5.0, purely job-mode spec
`flowx_testing/038_rec_003_precomputed_hash.json` stopped validating and so could no longer be
onboarded at all, because `02_onboarding_engine.py` raises on any non-empty `errors` list.

**Fix, and the rule going forward:** route every such finding through
`spec_validator.py::_append_cycle_finding(pipeline_mode, execution_mode, message, errors)`, which
appends an **ERROR** in pipeline mode and a **WARNING** in job mode. More generally: before making
any new check a hard error, ask which already-onboarded specs it would now reject. A validator
change is a compatibility change.

---

### 31. `TRUNCATE_AND_LOAD` is a full recompute, not an append — never stream from it

`TRUNCATE_AND_LOAD` looks append-shaped in the strategy table (`cdc/dispatcher.py` treats it as a
no-op, exactly like `APPEND`), but its target is a `@dlt.table` fed by a **full recompute**: every
update replaces the table's entire contents. Delta refuses to stream from a table whose history
contains non-append commits, so a downstream `readStream` over it fails with
`DELTA_SOURCE_TABLE_IGNORE_CHANGES`.

**What actually shipped (v1.5.0, defect D5):** `engine/source_plane.py`'s
`_NON_APPEND_ONLY_CDC_STRATEGIES` — the set backing the **G-STREAM** plan-time guard — listed only
`SCD1`/`SCD2`/`SCD3`/`FULL_SNAPSHOT_CDC`. A reconciliation flow whose source is an in-graph
`TRUNCATE_AND_LOAD` target therefore passed every plan-time check under
`execution_mode: "pipeline"` and then failed at pipeline **runtime** with
`DELTA_SOURCE_TABLE_IGNORE_CHANGES`. `TRUNCATE_AND_LOAD` is now in that set, and G-STREAM's message
names `execution_mode: "pipeline_audit_only"` as the correct setting for such a flow.

This is the live shape of the geneva scenario:
`flowx_testing/053_geneva_e41a47ba_recon_in_pipeline.json` uses `pipeline_audit_only` precisely
because its recon source (`geneva_admin.stg_tariffelementband`) is that same group's own
`TRUNCATE_AND_LOAD` ingestion target. Pinned by `tests/unit/test_geneva_e41a47ba_topology.py`.
That scenario is verified **offline only** (validator + `plan_source_plane`); see blocker B2 in
`RELEASE_NOTES.md` — the pipeline's run-as identity lacks table-level `SELECT` on the recon target,
so it has never been proven by a live run.

**The rule:** "does this strategy append only?" is the question G-STREAM asks, and `APPEND` is the
only ingestion/transformation strategy that answers yes. When adding a strategy, decide its answer
explicitly rather than by omission — an absent name defaults to "streamable", which is the unsafe
direction.

---

### 32. `spark.catalog.currentDatabase()` is NOT the pipeline's target schema during graph definition

At graph-definition time inside a Lakeflow pipeline, `spark.catalog.currentDatabase()` returns the
session's current database, which is **not** the schema the pipeline publishes to — even for a
pipeline whose resource definition plainly declares `schema: bronze_excalibur`.

**What actually shipped (v1.5.0, defect D6):** `notebooks/03_engine/03_lakeflow_declarative_pipeline.py`
gave `PIPELINE_CATALOG` a three-step fallback chain but set `PIPELINE_SCHEMA = _CURRENT_SCHEMA`
outright. `PIPELINE_SCHEMA` came out `None`, and the first `_node_name()` call in
`reconciliation/graph_registration.py` died with
`ValueError: Unsafe or malformed target_schema: None` — hitting any reconciliation flow that sets
no explicit `publish_schema`.

**The resolution order now used, and the one to copy:**

```
spark.conf "pipelines.schema"   →  spark.conf "pipelines.target"  →
spark.catalog.currentDatabase() →  GROUP_ROW.target_schema        →  warn
```

`pipelines.schema` is the current conf key; `pipelines.target` is its pre-`schema` spelling, still
set by older pipelines, so both must be tried. `currentDatabase()` stays in the chain only as a
late fallback, never as the first answer.

---

### 33. A new job must delegate onboarding to the generic `onboarding_job`, never inline `02_onboarding_engine.py`

A new job resource under `resources/` must **not** carry its own `notebook_task` pointing at
`notebooks/02_onboarding/02_onboarding_engine.py`. Each inlined copy pins its own widget names, its
own notebook path and its own cluster/environment settings, so any change to the onboarding
entrypoint has to be replayed across every one of them — which is exactly how the ~20 legacy
`flowx_test_*_job.yml` files drifted apart.

Delegate instead, via `run_job_task`, to the parameterised `resources/flowx_config_jobs/onboarding_job.yml`:

```yaml
- task_key: onboard_x
  run_job_task:
    job_id: ${resources.jobs.onboarding_job.id}
    job_parameters:
      spec_file_path: "${workspace.file_path}/flowx_testing/<spec>.json"
      catalog: flowx
      env: dev
      action_type: CREATE
```

Applied to `resources/feature_tests/flowx_test_recon_dag_job.yml`,
`resources/feature_tests/flowx_test_dag_001_unified_job.yml` and
`resources/bt_tests/flowx_test_104_geneva_tariffs_recon_job.yml`. The pre-existing legacy jobs are
deliberately **left as-is** — `onboarding_job.yml`'s own header records the standing "keep legacy
jobs as-is, add new orchestration alongside" decision. `resources/flowx_config_jobs/framework_config_onboarding_job.yml`
is the sibling that onboards a whole **directory** (`spec_dir`) rather than one spec.

Related trap in the same file family (v1.5.0, defect D7): when a flow is flipped to
`execution_mode: "pipeline"`, that job's **standalone** reconciliation task must be deleted.
`resources/feature_tests/flowx_test_002_003_job.yml` still carried `run_003_reconciliation` even though its own
header claimed the task had been removed — reconciliation would have run **twice** per trigger,
once in-pipeline and once as the job task, risking a double-append into the correction target.

---

### 34. `resources/` is grouped into subfolders — a new resource needs `../../`, and a commented-out `include:` DELETES resources

`resources/` is no longer flat. Every resource YAML lives one level down, in the folder matching its
purpose, and `databricks.yml`'s `include:` lists each group:

| Folder | Holds |
|---|---|
| `resources/flowx_bootstrap/` | the UC containers other resources live in: the `config`/`${var.schema}`/`flowx_sample` schemas + the sample suite's four Volumes (**not** the catalog — see pitfall 40) |
| `resources/flowx_bi/` | the AI/BI control-metadata dashboard |
| `resources/flowx_app/` | the Onboarding App + the UC Volume it stores authored specs in |
| `resources/flowx_config_jobs/` | `onboarding_job` (one spec per run) and `framework_config_onboarding_job` (a whole `spec_dir` per run) |
| `resources/observability/` | the DLT observability export job + the OTEL streaming pipeline |
| `resources/bt_tests/` | tests driven by real BT fixtures/pipelines: geneva tariff recon replay, ASN.1 decode, PGP decrypt |
| `resources/feature_tests/` | the `TC-*` feature/regression corpus — one job + one pipeline per case |
| `resources/stability_tests/` | reserved for `STABILITY_TEST_PLAN.md`'s A1–G4; empty today (see its `README.md`) |

Two traps follow from it:

**Relative paths are now two levels up.** DABs resolves a relative path against the file that
declares it, so every path in a resource YAML is `../../`, not `../`:

```yaml
notebook_path: ../../notebooks/02_onboarding/02_onboarding_engine.py   # NOT ../notebooks/...
dependencies:
  - ../../dist/*.whl                                                   # NOT ../dist/*.whl
```

A `../` left behind does **not** fail `bundle validate`; it fails at deploy with a missing-file
error, or worse, silently uploads nothing where a glob matched nothing.

**Never scope a deploy by editing `include:`.** DABs treats a resource absent from the config as one
to **delete from the target** — commenting out `resources/feature_tests/*.yml` to "deploy just the
app" destroys 83 deployed jobs and pipelines on the next `bundle deploy`. Scope with `--select`
(CLI ≥ v1.13.0) instead, which leaves unselected resources untouched:

```bash
databricks bundle deploy -t dev_flowx -p dev_flowx   --select apps.flowx_onboarding_app,jobs.onboarding_job,jobs.framework_config_onboarding_job,volumes.onboarding_specs_volume
```

The wheel is still built and uploaded under `--select`, because the selected jobs declare it in
`environments[].spec.dependencies`. Keep `jobs.onboarding_job` and `volumes.onboarding_specs_volume`
in the list even for an app-only change: the app resolves `${resources.jobs.onboarding_job.id}` into
its `FLOWX_ONBOARDING_JOB_ID` env var and binds the spec Volume to its service principal.

---

### 35. Intermediates are temporary, not views and not published tables — and the v1.6.0 upgrade renames them

The v1.6.0 **Intermediate Object Rule**: an intermediate dataset is a `@dlt.view` when it has one
reader, and a pipeline-scoped `@dlt.table(temporary=True)` under its **bare name** when
materialization is required (multi-reader read-once, or an API that needs a table). Only final
sinks are durable published tables. Three traps hide in that sentence:

**"Temporary" is not "view".** A `@dlt.view` is inlined into every consumer — two consumers of one
view open two physical reads (pitfall 24 / docs/13 L7). A temporary table is materialized once and
shared; it just never appears in Unity Catalog. Do not "simplify" a temporary table into a view:
you would silently reintroduce the duplicate-read bug the materialization exists to fix.

**Not everything can be temporary.** `dlt.create_streaming_table` has **no** `temporary` parameter,
so `_<t>_scd2_history` stays a real published table; `_<t>_snapshot_input` stays published because
`apply_changes_from_snapshot` needs a stable, versioned Delta relation and its name resolution has
burned this repo before (pitfall 23 / `cdc/snapshot.py`); a **healing** reconciliation flow's
`_recon__*__src` and healing `__tgt` stay published because the L5 `foreach_batch_sink` handler
reads them back with a plain `spark.read.table(...)`, which resolves through the metastore and
cannot see a temporary table. Any guard or "cleanup" written to the blanket rule fails against the
framework's own exceptions — check the code, not the slogan.

**The upgrade is a rename.** Pre-v1.6.0, a materialized `_<t>_staged` published a qualified
`catalog.schema` table, every L0 plane node published into the pipeline's schema, and
`recon__*__classified` was a published table. The first post-upgrade update re-registers all of
them under new (bare/underscore-prefixed) identities: streaming checkpoint state resets, published
intermediates drop out of UC, and an `APPEND` final target can re-ingest. Read
`docs/13_known_limitations_and_gotchas.md` **O7** before deploying v1.6.0 over a pipeline that has
history. To keep an L0 node queryable from outside the pipeline, set `source_plane.catalog` AND
`source_plane.schema` — since v1.6.0, `null` means *unpublished*, no longer "the pipeline's own
schema".

---

### 36. Reconciliation logging: `run_log_capture` gates `reconciliation_result` + dataset registration (v1.6.0), and both flags default to FALSE (v1.7.3)

Pre-v1.6.0, `logging_config` gated only `reconciliation_run_log`/`reconciliation_mismatch_log`
rows; `reconciliation_result` was written for every run "so a silenced flow still leaves a
record", and the `recon__*__metrics`/`__mismatch` datasets always existed. v1.6.0 makes the flags
mean what they say:

- `run_log_capture: false` → no `reconciliation_run_log` row, **no `reconciliation_result` row**
  (FAILED runs included), and — in pipeline mode — **no `recon__*__metrics` dataset at all**.
- `mismatch_log_capture: false` → no `reconciliation_mismatch_log` row and no `recon__*__mismatch`
  dataset.
- Both false → reconciliation persists **only** to its business targets. Healing still works (the
  heal ordering edge anchors on the always-registered `__classified`, not on `__metrics`).

Two things break if you assume the old contract:

1. **A monitoring query that expects a `reconciliation_result` row per run** silently sees nothing
   for a suppressed-logging flow. The failure signal for such a flow is the job/pipeline run state
   and the structured log events — by design.
2. **Two spec combinations are now rejected** (at onboarding by
   `spec_validator.py::_validate_logging_config` AND at graph definition, which also catches the
   `dataflow.recon.run_log_capture`/`dataflow.recon.mismatch_log` pipeline-conf overrides):
   `dq_config.rules` with `run_log_capture: false` (the expectations attach to `__metrics`, which
   would not exist — silently dropping declared DQ checks is the thing this framework never does),
   and `execution_mode: "pipeline_audit_only"` with both flags false (audit-only exists solely to
   produce the audit datasets — that combination is compute with no output).

**v1.7.3 raises the stakes: these flags now DEFAULT TO FALSE.** Through v1.7.2 a flow that never
mentioned `logging_config` got both flags `true`, so everything above described an opt-OUT. It is
now an opt-IN: silence is the default, and every consequence above applies to flows whose authors
wrote nothing at all. Concretely:

- Any **BI dashboard, alert or monitoring query** over `reconciliation_run_log`,
  `reconciliation_mismatch_log` or `reconciliation_result` goes **empty** for every flow that has
  not explicitly opted in — including flows onboarded before v1.7.3 and re-onboarded after it.
- The two rejected combinations become reachable **by omission**. A flow with `dq_config.rules`
  and no `logging_config` is now rejected, as is a `pipeline_audit_only` flow with no
  `logging_config`. This is deliberate — the alternative is silently dropping declared DQ
  expectations — and the error message names whether the flag was written `false`, left unset (and
  defaulted false by v1.7.3), or forced false by a `dataflow.recon.*` pipeline-conf override, then
  tells you to set `run_log_capture: true`.
- **Migration:** add `"logging_config": {"run_log_capture": true, "mismatch_log_capture": true}` to
  every reconciliation flow whose audit trail anything depends on, and re-onboard.

Only the implicit fallback moved. A flow that already writes an explicit `true`/`false`, and any
run that sets the `recon_run_log_capture`/`recon_mismatch_log` job parameters, resolve exactly as
before.

The backstop export (`observability/reconciliation_export.py`) follows the same contract: it
probes `__metrics` when `run_log_capture` is on (else `__mismatch`), skips both-flags-false flows,
and gates its `reconciliation_result` writes under `run_log_capture`.


---

### 37. A real ASN.1 module's natural top-level PDU is a `CHOICE` — and `CHOICE` is rejected anywhere in the tree

`asn1_pdu_name` must name a **top-level `SEQUENCE`**, and `derive_asn1_field_defs` resolves that
`SEQUENCE`'s whole member tree before it will emit a Spark schema. `CHOICE` raises at any depth:

```
Asn1DecodeError: ASN.1 CHOICE types are not yet supported by schema auto-derivation (node: 'CallEventDetail')
```

The trap is that toy fixtures never show this. `sample_data/asn1_schema/gsm_cdr.asn` is one flat
5-field `SEQUENCE`, so `GsmCallDetailRecord` "just works" and nothing warns you that the pattern
does not carry over. Every genuine telecom module is built the other way round — a root `CHOICE`
selecting between message kinds. In `BT_Usecase/UC7/data/asn_schema/TAP.310.asn1` (the real GSMA TAP
release 3.10 spec):

* `DataInterChange` — the module's own top-level PDU — is `CHOICE { transferBatch, notification }`.
  **Rejected.**
* `TransferBatch` *is* a `SEQUENCE`, but reaches `CallEventDetailList` → `CallEventDetail`, also a
  `CHOICE`. **Rejected**, one level deeper, which is the version of this that wastes an afternoon.
* `Notification` (`[APPLICATION 2] SEQUENCE`) resolves completely — and is a real TAP3 file-level
  PDU, not a contrivance. This is what `resources/sample_jobs/onboarding/sample_06_asn1_tap3_ingestion.json`
  uses.

**Do not read the module to find a candidate.** Ask the framework's own resolver, which is the
authority and takes about a second over 375 types:

```python
import asn1tools
from flowx.lakeflow_framework.asn1.decoder import derive_asn1_field_defs

module = "BT_Usecase/UC7/data/asn_schema/TAP.310.asn1"
types = next(iter(asn1tools.parse_files([module]).values()))["types"]
for name, node in types.items():
    if node.get("type") != "SEQUENCE":
        continue
    try:
        fields = derive_asn1_field_defs(module, name)
        print(f"{name:40s} {len(fields)} fields")
    except Exception:
        pass          # CHOICE / recursion / unresolvable member somewhere in the tree
```

On TAP.310 that prints 70 usable PDUs out of 93 top-level `SEQUENCE` types. Two related notes:

* **One module per file.** `derive_asn1_field_defs` refuses a file defining more than one ASN.1
  module, and does not follow `IMPORTS` across files.
* **A nested `SEQUENCE` becomes a `struct`, a `SEQUENCE OF` an `array`** — they are not flattened.
  `Notification` yields scalars, three `struct<localTimeStamp,utcTimeOffset>` timestamps, and an
  `array<string>`. Flatten downstream with `explode_columns` if a spec wants columns.


---

### 38. A `FULL_SNAPSHOT_CDC` ingestion flow is NOT in the source plane — never `bind()` for one

`engine/source_plane.py::_plan_ingestion_consumers` skips every row whose `cdc_load_strategy` is
`FULL_SNAPSHOT_CDC`, because `apply_changes_from_snapshot`'s lambda reads a **path**, not a
pipeline dataset (pitfall 3). So the plan contains no consumer under `"<dataflow_id>:source"`, and
`bind()` — which never guesses — can only raise:

```
FrameworkConfigError: source_plane.bind: unknown consumer_id 'df_sample_01_part_snapshot_ingest:source'.
Known consumer ids: ['df_sample_01_customer_scd1_ingest:source', ...]
```

**The half that is easy to miss:** the exclusion comment talks about the *lambda*, but a snapshot
flow still has a **staged view in the graph** — `_<t>_staged` → `_<t>_clean` →
`_<t>_snapshot_input` → target — and that view needs a real source read. Until v1.6.1
`flow_generators.py::_build_ingestion_dataframe` called `bind()` unconditionally, so **every**
`FULL_SNAPSHOT_CDC` pipeline in the repo died at graph analysis. It went unnoticed because the
v1.6.0 source-plane work was never deployed; Sample 01 hit it on the first live run (2026-09-01).

The shape now in place:

```python
if is_snapshot_cdc:                       # target_config.cdc_load_strategy == FULL_SNAPSHOT_CDC
    staged_df = read_ingestion_source(spark, flow_row.source_type, source_config)
else:
    staged_df = bind(plan, source_consumer_id, want_stream=is_streaming)
```

Reading directly is correct rather than merely expedient: the plane exists to make a *shared*
locator read once (R2), and a snapshot source has exactly one consumer by construction — that is
precisely what the exclusion asserts. Both sides branch on the single exported constant
`source_plane.SNAPSHOT_EXCLUDED_FROM_SOURCE_PLANE`, never on two copies of the string, because a
drift between them is invisible until pipeline runtime. Pinned by
`tests/unit/test_flow_generators.py` §6 — including the other half, that every non-snapshot
strategy *still* binds.

---

### 40. A Volume needs its SCHEMA declared — but declaring the CATALOG is worse than not declaring it

Two halves, learned in one deploy on 2026-09-02. The first half is the obvious fix; the second is
the trap.

**The bug.** A first deploy to a fresh workspace died on its very first resource:

```
Error: cannot create resources.volumes.framework_wheels_volume:
       Schema 'flowx.config' does not exist
```

The bundle declared its two Volumes but nothing declared the schema holding them. On the older
workspaces `flowx.config` had been created by hand long ago, so the gap was invisible — the
bundle only ever deployed where a human had already run the `CREATE`.

**The fix.** Declare the schemas (`resources/flowx_bootstrap/flowx_schemas.yml`). DABs
rewrites each Volume's literal `schema_name` into `${resources.schemas.config_schema.name}` — a
real dependency edge, visible in `bundle validate -o json` — so schema-before-volume is enforced by
DABs itself, not by convention.

**The trap: do NOT complete the hierarchy with a `catalogs.*` resource.** It looks like the
obvious next step. It cannot work here, for two independent reasons:

```
Error: cannot create resources.catalogs.flowx_catalog:
       Metastore storage root URL does not exist. Default Storage is enabled in your account.
       ... please provide a storage location for the catalog (400 INVALID_STATE)
```

1. These accounts use UC **Default Storage** — the metastore has no storage root, so
   `CREATE CATALOG` with no `MANAGED LOCATION` is rejected. Supplying one is not a fix either: a
   Default-Storage catalog's `storage_root` is an **account-managed** path carrying metastore and
   catalog UUIDs generated at create time (`s3://dbstorage-prod-ycljl/uc/9acb37d2-.../`). It cannot
   be committed to YAML and differs per workspace.
2. `flowx` already exists on all three targets anyway, each created outside the bundle.

**And the failure is not contained** — this is the part worth remembering. DABs propagates it down
the dependency edge it just created, so the unbuildable catalog took the schemas with it:

```
Error: cannot create resources.schemas.config_schema:
       dependency failed: resources.catalogs.flowx_catalog
```

A resource that can only ever fail is strictly worse than no resource: it converts a working deploy
into a failing one *and* blocks the schemas that were the actual fix. The catalog is a documented
**prerequisite** — created once per workspace in the UI (Catalog → Create catalog → Default
storage). `tests/unit/test_resource_layout.py::test_no_catalog_is_declared_as_a_bundle_resource`
asserts it stays undeclared.

**Two related mechanisms not to conflate.** `Schema ... does not exist` is resource-creation order,
fixed by declaring the schema. `volume <catalog>.config.wheels does not exist at
workspace.artifact_path` is a *config-resolution pre-check* that runs before any resource is
created, so a Volume declared in the same bundle can never satisfy it — the one-time
comment-out/deploy/uncomment bootstrap in `databricks.yml` survives, and was re-verified on CLI
v1.13.0 with the schema resources in place.

**Bind, do not re-create, anything that already exists.** `flowx.config` predates the group on
`dev_flowx` and `hoonartek`, so DABs does not own it there:

```
databricks bundle deployment bind schemas.config_schema flowx.config -t dev_flowx -p dev_flowx
```

---

### 39. A module loaded from a JOB task must not transitively `import dlt`

`import dlt` is not free outside a Lakeflow pipeline. It calls into the runtime's notebook entry
point, which in a plain **job** notebook task has no notebook id to return, and the import dies
before any framework code runs:

```
Py4JJavaError: An error occurred while calling o36.get.
: java.util.NoSuchElementException: None.get
    at scala.None$.get(Option.scala:627)
```

Confirmed live 2026-09-01: `observability/reconciliation_export.py` imported one *pure*
exception-inspection helper (`_is_table_not_found`) from `dq/quarantine.py`, which does a
module-level `import dlt`. Sample 03's `observability_export` task failed at **import time**,
taking `store_sample_config` with it — while the pipeline that produced its reconciliation
datasets had succeeded minutes earlier. `reconciliation/appender.py` carried the identical latent
import, which would have broken the standalone reconciliation job the same way.

**The trap is transitivity.** Reviewing the diff of `reconciliation_export.py` shows a helper
import from a sibling module and nothing alarming; the `dlt` edge is one hop further out. The
fix is not to shuffle the import site but to put shared code in a module that cannot drag `dlt`
in: `dq/table_errors.py` now holds `is_table_not_found` / `TABLE_NOT_FOUND_CONDITIONS` and
imports **nothing at all**. `dq/quarantine.py` keeps `_is_table_not_found` as an alias for
pipeline-side callers — and a job-context module importing *that alias* would reintroduce the bug
in full, which is why the alias carries a comment saying so.

**This does not fail locally.** `databricks-dlt` is a dev dependency, so `import dlt` succeeds in
pytest and proves nothing about the serverless job runtime. The guard is therefore static:
`tests/unit/test_job_context_has_no_dlt_import.py` walks the real import graph with `ast` and
asserts the edge is absent, with `dq/quarantine.py` itself as the control so the assertions
cannot pass vacuously.

Job-context entry points to keep clean (each is loaded by a notebook task, not a pipeline):
`observability/reconciliation_export.py` (`08_dlt_observability_engine.py`),
`reconciliation/appender.py` (`05_reconciliation_engine.py`), and `dq/table_errors.py` itself.

---

### 41. An attribute the framework does not read is now REJECTED — generated specs must be validated, not eyeballed

**Symptom (pre-v1.7.1, the reason this exists).** A spec onboards cleanly, the control-table row
is written, the pipeline runs green — and a feature the spec plainly declares never happens. No
error, no warning, nothing in the event log. The classic shapes, all observed in generated specs:

| In the spec | What the framework did |
|---|---|
| `"data_quality": {...}` | The real key is `dq_config`. **No DQ rule ever ran.** |
| `"cdc_config": {"keys": [...]}` | v1 shape. **CDC keys ignored**; the strategy fell back to whatever `target_config.cdc_load_strategy` said, or failed its own required-field check for an unrelated reason. |
| `"partition_by": [...]` | The real key is `target_config.partition_columns`. **Table not partitioned.** |
| `"infer_schema": true` | Never a real key. Silently nothing. |

Only 8 of the JSON schema's 103 object definitions set `additionalProperties: false`, so the
schema did not catch these either — a spec with six invented keys returned `valid: true`,
`error_count: 0`.

**As of v1.7.1** `reject_unknown_keys()` makes every one of these a hard error, on the same
presence-not-truthiness rule as `reject_removed_keys()`: `"infer_schema": false` is rejected
exactly like `true`, because it is still a statement about a feature that does not exist. The
JSON schema now sets `additionalProperties: false` on every authored container too.

**Exempt:** any key matching `^_` (author comment — `_scenario`, `_provenance`, `_test_case_note`;
JSON has no comment syntax and the specs in `flowx_testing/` lean on these heavily) and
`$schema`.

**The two traps when adding an attribute.**

1. Add it to *both* the validator allowlist and `onboarding_spec.schema.json`, or
   `test_unknown_key_rejection.py::test_allowed_key_sets_match_json_schema` fails. That test is
   deliberate: an attribute in the schema but not the allowlist is rejected at runtime despite
   the schema promising it works.
2. If the attribute is authored anywhere in `databricks-app/`, the app emits it and onboarding
   now rejects it. Rebuild the web bundle — see the "Definition of Done" step 5 in `AGENTS.md`.

**Generating a spec.** Do not compose one field-by-field from memory; that is what produces these
keys. Copy the closest of the five machine-validated examples in
`agent_skills/reference/golden_specs.json`, change the values, then validate — offline, no
cluster, sub-second:

```python
import sys; sys.path.insert(0, "src")
from flowx.lakeflow_framework.onboarding.agent_tools import validate_json
print(validate_json(open("my_spec.json", encoding="utf-8").read())["summary"])
```

`validate_spec(None, spec)` works for every flow type as of v1.7.1 — before that, any
transformation or reconciliation flow raised `AttributeError: 'NoneType' object has no attribute
'sql'` from the `EXPLAIN` check, which surfaced as `unexpected error during validation` and made
those flows impossible to lint offline. The `EXPLAIN` check is now skipped without a session
(parameter substitution is still checked) and runs at onboarding time on the cluster.


### 42. Spark's plain `rate` source is a race under `AvailableNow` — a one-row pulse must be `rate-micro-batch`

`spark.readStream.format("rate")` counts rows as **wall-clock seconds since its checkpoint was
created**. In a triggered pipeline update (`Trigger.AvailableNow`) that makes the row count
nondeterministic. Measured live, both variants feeding counting handlers side by side:

| Pulse source | Fresh checkpoint | Incremental update |
|---|---|---|
| `rate` (`rowsPerSecond=1`, `.limit(1)`) | 1 row | **0 rows** |
| `rate-micro-batch` (`rowsPerBatch=1`) | 1 row | 1 row |

The v1.7.5 `export_trigger: "per_update"` pulse shipped first with `rate`, backed by a 3-run probe
that happened to get 1 row every time. UC6's first green update then produced correct gold tables
and **four empty sinks**: the pulse emitted 0 rows, the join emitted 0 rows, the `pgp_zip` writer
deleted its zero-row parts and wrote no file. Silent missing output — the exact failure the trigger
exists to prevent. `rate-micro-batch` emits exactly `rowsPerBatch` rows per micro-batch regardless
of the clock. Do not add `.limit()` (a streaming LIMIT is its own risk).

**Migration trap when swapping sources**: an existing pulse checkpoint holds the `rate` offset format, so the first incremental update after the change fails with `No usable value for offset` and DLT retries into the same wall. And it compounds one hop downstream: RENAMING the pulse (v1.7.5 moved from one pulse per sink to one shared `_flowx_export_pulse`) changed the Delta table id each sink flow streams from, so every sink flow's own checkpoint failed with `DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE` -- and a sink flow is not a selectable table, so the remedy is a whole-pipeline `full_refresh=True` (264s live; bronze re-read six small files). Both are one-time migrations; fresh pipelines are unaffected.

Two lessons beyond the fix. "Verified across N runs" is weak evidence for anything time-based —
vary the condition (fresh vs incremental checkpoint) before trusting it. And **streaming flows never
populate `flow_progress.metrics.num_output_rows` in `event_log()`** here: even bronze tables with
known rows report `None`. A zero-row streaming flow is therefore invisible in the event log; count
rows in a handler or inspect the sink's output instead. Batch flows (MVs) do report the metric.

### 43. Both validation gates pass and the pipeline still fails — the third gate is graph planning

`spec_validator.py` and the JSON schema check *shape*. Neither plans the Lakeflow graph, so a class
of defect passes both and fails only at pipeline runtime, ~8–10 minutes per cycle, **one at a time**
(graph definition is all-or-nothing). UC6 hit five in a row:

1. Sinks streaming from an aggregating MV — G-STREAM (`source_plane.py`). Fix: `export_trigger: per_update`.
2. Five plain-gzip sources with `path` pointing at `_extracted/<name>/`, which nothing writes — Auto
   Loader's `CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE`. A plain `.gz` reads from `raw/` directly.
3. `schema_config_path` files never uploaded to the volume.
4. Reconciliation `match_keys: ["__framework_hash_key"]` — circular. The matcher HASHES `match_keys`
   into that column itself (`matcher.py:181`); give it real business columns.
5. A `UNION ALL` of one streaming branch with two batch branches, and a `ROW_NUMBER()` window over a
   stream — both rejected by Spark at execution, both invisible to the validator.

Before the first run of a new spec: (a) every `source_config.path` is a directory that will contain
files, and every `schema_config_path` is uploaded; (b) trace each `is_streaming: true` input to its
producer — if the producer is a `materialized_view` or `TRUNCATE_AND_LOAD`, it will fail; (c) every
`match_keys` column exists on the table; (d) any flow with `UNION ALL` or a window function has ALL
inputs batch. And tag governance only where a table exists: `apply_all_governance_tags` now skips
`target_type: "sink"` (no materialized table) — before v1.7.5 one sink's `TABLE_OR_VIEW_NOT_FOUND`
failed the whole group's tagging.

### 44. SQL over the control tables and system tables: five ways to be confidently wrong

Building `<catalog>.observability` (`control_plane/observability_views.py`) surfaced five defects
that all **returned a plausible answer rather than an error** — the kind that survives review and
ships. Every one was found only by running against a live workspace, not by any local test.

1. **Enum-ish control-table values are UPPERCASE.** `cdc_load_strategy` is `APPEND`, `SCD1`,
   `SCD2`, `TRUNCATE_AND_LOAD`. A lowercase-only `RLIKE '(scd|cdc|merge|truncate)'` reported
   "no CDC" for every SCD group. Wrap in `LOWER()`.
2. **`SPLIT('', ', ')` returns `['']`, and `ARRAY_COMPACT` will not remove it.** ARRAY_COMPACT
   drops NULLs only, so the empty element survives, sorts first, and `CONCAT_WS` renders a
   leading `", "` on the merged list. Use `FILTER(arr, x -> x IS NOT NULL AND x <> '')`.
3. **`*_config_json` columns are frequently the literal string `'{}'`.** Testing
   `IS NOT NULL AND <> ''` counts an empty JSON object as configured, which reported
   "data quality enabled" for every group. Use `TRIM(col) NOT IN ('', '{}')`.
4. **The newest `flow_progress` event is not necessarily the terminal one.** A streaming flow
   can emit its last progress event mid-batch, so `ORDER BY event_time DESC` yields a partial
   `num_output_rows` and a `RUNNING` status for an update that finished. Rank terminal statuses
   (`COMPLETED`/`FAILED`/`EXCLUDED`/`SKIPPED`) first, then by time. And event-log counts are
   **cumulative within an update**, so keep exactly one event per `(update_id, flow_name)` or
   `SUM(rows_written)` multiplies throughput by the number of progress events.
5. **`system.lakeflow.pipelines.settings` has no `catalog` or `target` field** — only `photon`,
   `development`, `continuous`, `serverless`, `edition`, `channel`. Reading `settings.catalog`
   fails with `FIELD_NOT_FOUND` and takes every dependent view down with it. A pipeline's
   event-log table location is not derivable from its row; discover it from
   `system.information_schema.tables` matching `event_log_<pipeline_id with '-'→'_'>`.

Two related attribution facts. **Notebook task `base_parameters` do NOT appear in
`system.lakeflow.job_task_run_timeline.task_parameters`** (verified empty), so a job run cannot
be tied back to its `dataflow_group_id` that way — a job **tag** is required, and
`resources/{uc3,uc6,uc7}/*_job.yml` now set one. Pipelines are fine as-is:
`system.lakeflow.pipelines.configuration['dataflow.group.id']` is already populated from each
pipeline resource's `configuration:` block.

### 45. `AI_FORECAST` on a daily series will embarrass you in a demo

On a young workspace `system.billing.usage` holds only a few days. Fitting `AI_FORECAST` to
**three daily points** projected **10,257,538,803 DBUs** by day 14 — no error, just a chart that
destroys credibility. Four rules, now encoded in both the dashboard SQL and the Genie space
instructions:

- **Forecast on an HOURLY series**: `date_trunc('HOUR', usage_start_time)` turned 3 usable points
  into 73 and produced a credible result (7.95 DBU/hr against a 6.71 observed mean).
- **Exclude the current, partly-elapsed bucket** (`< date_trunc('HOUR', current_timestamp())`),
  at the *same grain as the aggregation*, or the last actual point dips and the forecast starts
  from an artificially low base.
- **Always pass `parameters => '{"global_floor": 0}'`.** Cost and counts cannot go negative;
  without the floor the lower confidence band does.
- **Report the band and the hours of history, never the point forecast alone.** A wide band means
  little history, not volatile spend — a completely different thing for the reader to act on. And
  say it is list price and a projection, not a commitment.

The `forecast-line` widget also needs a **bridge row**: one row at the last actual timestamp with
the actual value copied into all three forecast columns, or the historical line and the forecast
band render with a visible gap between them.
