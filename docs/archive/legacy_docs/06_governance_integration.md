# Governance Integration

See also: [README.md](README.md) for the full Metaflow documentation index.

**Metaflow applies governance as a tags-only model.** The framework applies key-value
Unity Catalog tags to columns and tables — it does **not** create, bind, or administer the
masking/row-filter *policy* that gives a tag its actual enforcement behavior. That's a
workspace admin's Unity Catalog tag-policy configuration, external to this repo.

Ground truth for everything below:
`src/NextGen_Metadata_Framework/lakeflow_framework/governance/tags.py` (full design
rationale in its module docstring), `control_plane/post_deployment.py::apply_all_governance_tags`,
`notebooks/04_governance/04_apply_governance_and_egress.py`, and
`onboarding/spec_validator.py::_validate_governance_tags`. Schema reference:
[01_control_metadata_schema.md §6](01_control_metadata_schema.md#6-governance_tags-ingestion-and-transformation-flows).

## 1. The `governance_tags` shape

```json
{
  "column_tags": [
    {"column": "ssn", "tags": {"mask": "PII", "pii_type": "SSN"}}
  ],
  "table_tags": {"row_filter": "region_restricted", "domain": "finance"}
}
```

| Attribute | Type | Required |
|---|---|---|
| `column_tags[].column` | string | **yes** |
| `column_tags[].tags` | object (string → string, any number of tags) | **yes** |
| `table_tags` | object (string → string, any number of tags) | no |

Both `column_tags[].tags` and `table_tags` accept **multiple tags per column and per
table** in one entry — Unity Catalog's `SET TAGS` clause takes any number of key/value
pairs in a single statement; only *multiple columns* need separate `ALTER TABLE`
statements (`governance/tags.py::apply_governance_tags` issues one
`ALTER TABLE ... ALTER COLUMN ... SET TAGS` per `column_tags[]` entry, plus at most one
`ALTER TABLE ... SET TAGS` for `table_tags`). An entry with an empty `tags` object is
silently skipped rather than issuing a no-op `ALTER TABLE`.

`governance_tags` is validated by `onboarding/spec_validator.py::_validate_governance_tags`
on both `ingestion_flow`/`transformation_flow` entries — it is a **key-value dict
container**, not a list of policy bindings, so validation is comparatively shallow: every
`column_tags[]` entry needs a non-empty `column` and a non-empty `tags` object (string
values only); `table_tags`, if present, must be string-to-string. `test_specs/spec_12_config_validation_negative.json`
has a worked negative example — a `column_tags` entry with `tags` but no `column` — which
produces:

```
ingestion_flow[df_bad_example].governance_tags.column_tags[0].column: is required but was missing or empty
```

Persisted as `governance_tags_json` on both `ingestion_flow_spec` and
`transformation_flow_spec` (see `control_plane/ddl_definitions.py`) — the same JSON shape
the onboarding spec used, round-tripped verbatim.

Real worked examples: `test_specs/spec_20_crypto_abac_exhaustive.json` (a `mask`/`pii_type`
column tag plus a `row_filter` table tag on the same transformation target — used
throughout this doc) and `test_specs/spec_24_new_27_08_test_flagship.json` (a `mask`
column tag plus a `domain` table tag on an ingestion target).

## 2. Why this can't run inside the DLT graph-definition code

`ALTER TABLE ... SET TAGS` / `ALTER TABLE ... ALTER COLUMN ... SET TAGS` are Unity Catalog
DDL statements against an *already-materialized* Delta table. A Lakeflow Declarative
Pipeline notebook executes in two phases (see [03_engine_execution_flow.md](03_engine_execution_flow.md)):
graph-definition, where `@dlt.table`/`@dlt.view` nodes are registered as lazy plans, and
graph-execution, where Lakeflow's own runtime actually materializes them. The target table
is not guaranteed to exist — or be in a stable, queryable state — until graph-execution has
*finished*. Calling `ALTER TABLE ... SET TAGS` during graph-definition would either fail
outright (the table doesn't exist yet) or race the table's own creation. Any DDL against a
target table — `SET TAGS` here, or `SET ROW FILTER`/`SET MASK` for a policy-binding
approach — is subject to this same platform constraint, regardless of which specific
statement is issued.

## 3. How and when it actually runs: a post-pipeline-update job task

`apply_governance_tags` (`governance/tags.py`) applies tags for a single, already-qualified
table. It is never called directly from onboarding or from the pipeline notebook — the
caller is `apply_all_governance_tags` (`control_plane/post_deployment.py`), which loops
every active `ingestion_flow_spec`/`transformation_flow_spec` row in a
`dataflow_group_id` and, for each row with a non-empty `governance_tags_json`, calls
`apply_governance_tags(spark, flow_row.target_catalog, flow_row.target_schema, flow_row.target_table, governance_tags)`.

That function is invoked from `notebooks/04_governance/04_apply_governance_and_egress.py`,
wired in `resources/metadata_framework_job.yml` as the `apply_governance_and_egress` task
with `depends_on: run_pipeline_update` and no `run_if`/manual-trigger condition — every
execution of `metadata_framework_job` applies governance for the configured
`dataflow_group_id` as an unconditional part of that one job run:

```
setup_control_tables -> seed_sample_data -> onboard_spec_01..06 (parallel)
  -> run_pipeline_update -> apply_governance_and_egress
```

There is no separate "governance job" a user has to remember to trigger. The notebook
gates the call behind an `apply_abac` widget (`true`/`false`, default `true` — a holdover
name; what it gates is tag application, not ABAC policy binding) so a job run can skip
governance entirely if needed.

The same notebook, same task, also runs `capture_all_scd_change_counts` (Phase 10 —
SCD/CDC insert/update/delete counts via Delta Change Data Feed, emitted as structured log
events) immediately after governance tagging, for the same underlying reason: a target
table's post-update Delta commit version is only knowable once the update has actually run
and committed, so it's a genuine post-deployment step too. It runs in its own `try`, not
nested inside governance's — a metrics-capture failure must never be conflated with, or
suppress, a tag-application failure. This doc covers governance only; the metrics side is
orthogonal and not further detailed here.

`notebooks/04_governance/04_apply_governance_and_egress.py` also still accepts (and
silently ignores) a `run_egress_exports` job parameter some older `resources/*.yml` job
definitions still pass — `external_sink`/`sink` egress does not run as a post-deployment
step at all; it's a genuine `dlt.create_sink` + `@dlt.append_flow` pair registered *inside*
the pipeline's own graph (`engine/sink_registration.py`). An unconsumed job
parameter is harmless (Databricks doesn't require every passed parameter to have a
matching widget) — mentioned here only so a reader doesn't mistake it for a governance
knob.

## 4. Idempotency: why no ledger is needed

An ABAC-style row-filter/column-mask binding approach needs its own idempotency tracking: a
fifth control table (`governance_applied_log`) and a dedicated `governance/idempotency.py`
module — `compute_policy_config_hash(function_name, using_columns)`,
`is_policy_already_applied(...)` checked before every `ALTER TABLE`, and
`record_policy_applied(...)` upserted after a successful one. The reason is structural, not
just "DDL might error on repeat": `ALTER TABLE ... SET ROW FILTER`/`SET MASK` binds a
*named UC function reference*, and Unity Catalog's own row-filter/mask metadata shape
varies across runtime versions — not something a framework should want to depend on to
answer "did we already apply this, with this exact configuration." A ledger like that would
be its own source of truth for that question, with a config hash so a changed
`function_name`/`using_columns` produces exactly one re-bind instead of an unconditional
re-application of every policy on every run.

None of that applies to tags. `ALTER TABLE ... SET TAGS (...)` is a plain key-value
upsert, not a function binding: re-applying an identical key/value pair is a no-op, and
applying a *changed* value for an existing key simply overwrites it — both are the
correct, final state, with no intermediate risk to guard against. And unlike row filters/
masks, Unity Catalog exposes current tag state through a stable, queryable system table —
`information_schema.column_tags`/`table_tags` — so there's no need for the framework to
maintain its own shadow ledger just to know what's currently applied; anyone (or any test)
can just query it directly. `governance/tags.py`'s module docstring states this plainly:
tag DDL is naturally idempotent, so no ledger table is needed. There is no
`governance/abac.py` or `governance/idempotency.py` module, and no no-op stub in their
place — see [01_control_metadata_schema.md §1](01_control_metadata_schema.md#1-the-control-tables),
which notes governance has no dedicated control table at all.

`notebooks/07_verification/07_verify_crypto_abac_exhaustive.py` Sections D and E exercise
this live, not just structurally: Section D calls `apply_governance_tags` a second time
with an unchanged `governance_tags` dict and asserts the tag values read back from
`information_schema` are unchanged (an idempotent re-run, not a ledger-skip — the
`ALTER TABLE` statement genuinely re-executes; it's just that its result is identical);
Section E changes `row_filter`'s value, re-applies, and asserts the new value landed via a
plain overwrite, then restores the original value so a later job re-run's own checks stay
consistent with what `spec_20` actually declares.

## 5. Framework-scope boundary: tags applied, not administered

The framework's job stops at `ALTER TABLE ... SET TAGS`. What (if anything) a tag *does* —
masking a column's value, filtering rows, routing a discovery/classification workflow — is
entirely defined by a Unity Catalog tag policy a workspace admin configures separately,
outside this repo. `governance_tags: {"column_tags": [{"column": "ssn", "tags": {"mask": "PII"}}]}`
declares a label; it is inert on its own. This is a deliberate scope boundary, not a gap:
policy creation/administration was explicitly called out as out-of-scope when the tags
model replaced ABAC binding (`onboarding/spec_validator.py::_validate_governance_tags`'s
own docstring states it verbatim).

**This workspace has a real, live, enforced Unity Catalog tag policy** restricting the
`mask` tag key to the values `PII` and `cost` only — a concrete, external example of a
tag-policy administrator constraining what values a given tag key may even take, confirmed
live in this environment. It's a genuine illustration that the tags-only design's division
of labor works end to end: the framework applies `mask: PII` (a value the live policy
permits — a spec author trying `mask: everything` on this workspace would fail at
`ALTER TABLE` time, not because the framework validates it, but because the workspace's own
tag policy rejects it), and the workspace's separately administered masking behavior then
does the actual redaction. `notebooks/07_verification/07_verify_crypto_abac_exhaustive.py`
proves this concretely (Section C): after `apply_governance_and_egress` tags
`silver_crypto_abac.customer_pii_authorized.ssn_plaintext` with `mask: PII`, reading that
column directly off the live table returns `***-**-1111` — not the plaintext value — even
though nothing in this framework ever issued a masking `ALTER TABLE ... ALTER COLUMN ...
SET MASK` call. The tag alone triggered live enforcement, driven entirely by the
workspace's own tag-policy configuration:

```python
golden_path_masked_values = {
    r["account_id"]: r["ssn_plaintext"]
    for r in spark.table(SILVER_TABLE).select("account_id", "ssn_plaintext").collect()
}
check(
    "silver_ssn_plaintext_is_actively_masked_by_a_live_uc_tag_policy",
    golden_path_masked_values.get("ACC001", "").startswith("***-**-"),
    ...,
)
```

**A note on `notebooks/04_governance/04b_provision_governance_udfs_and_entitlements.py`**:
this notebook still exists and still provisions `fn_crypto_abac_region_filter`/
`fn_crypto_abac_mask_ssn` UC functions plus a `user_entitlements` table — but it predates
the tags-only model and is kept only as a **reference example** of what a genuinely
attribute-based (`current_user()`-driven) enforcement function looks like, useful if a
workspace admin wants to write a tag policy that calls something like it.
`apply_governance_tags` never wires `governance_tags` to these functions automatically —
don't confuse this notebook's presence with the tags model actually binding anything; the
masking proven in
Section C above comes from the workspace's own tag policy on the `mask` key, not from
`fn_crypto_abac_mask_ssn`.

## 6. Error handling

`apply_governance_tags` applies every configured tag independently and best-effort: a
failure applying one column's tags, or the table's tags, does not prevent the others from
being attempted. All failures are collected and raised together at the end as a single
`AbacApplicationError` (the exception class kept its original name — it now covers tag-
application failures rather than policy-binding failures) naming the exact
column/table and the failing tag values, e.g.:

```
2 governance tag application failure(s) on poc.silver_crypto_abac.customer_pii_authorized:
["column_tags {'column': 'does_not_exist', 'tags': {...}} on poc...: ...", "table_tags {...} on poc...: ..."]
```

`catalog`/`schema`/`table`/`column` identifiers are passed through
`crypto/secrets.py::assert_safe_identifier` before being interpolated into DDL (rejecting
anything that isn't a safe identifier — `notebooks/07_verification/07_verify_crypto_abac_exhaustive.py`
Section F proves this live against an unsafe `region_code; DROP TABLE x--` column name,
raising `AbacApplicationError` rather than executing it). Tag **keys and values**, by
contrast, are free-form user strings (a tag value like `"O'Brien's team"` is a plausible
legitimate value, not an attack), so they're SQL-escaped by doubling embedded single
quotes rather than validated as identifiers (`governance/tags.py::_render_tags_clause`).

`apply_all_governance_tags` itself does not add its own try/except around each flow row —
a governance-tag failure for one flow's target table propagates and fails the whole
`apply_governance_and_egress` job task, surfacing as a failed Databricks job run rather
than a silently-skipped flow.

## 7. A complete example

From `test_specs/spec_20_crypto_abac_exhaustive.json`'s transformation flow — a `row_filter`
table tag and a `mask`/`pii_type` column tag on the same target the live masking policy
above enforces:

```json
{
  "flow_step_id": "ts_crypto_abac_silver_authorized",
  "dataflow_id": "df_crypto_abac_bronze_ingest",
  "target_catalog": "{{catalog}}",
  "target_schema": "silver_crypto_abac",
  "target_table": "customer_pii_authorized",
  "target_type": "streaming_table",
  "source_inputs": [ { "input_name": "bronze_pii", "table": "{{catalog}}.bronze_crypto_abac.raw_customer_pii", "is_streaming": true } ],
  "transformation_sql": "SELECT account_id, region_code, ssn_for_gcm, ssn_for_cbc, ssn_for_ecb, ssn_override_ciphertext FROM bronze_pii",
  "target_config": { "storage_format": "delta", "cdc_load_strategy": "APPEND" },
  "dq_config": {},
  "governance_tags": {
    "table_tags": { "row_filter": "region_restricted" },
    "column_tags": [
      { "column": "ssn_plaintext", "tags": { "mask": "PII", "pii_type": "SSN" } }
    ]
  }
}
```

Once `run_pipeline_update` materializes `customer_pii_authorized`, `apply_governance_and_egress`
issues:

```sql
ALTER TABLE poc.silver_crypto_abac.customer_pii_authorized SET TAGS ('row_filter' = 'region_restricted');
ALTER TABLE poc.silver_crypto_abac.customer_pii_authorized ALTER COLUMN `ssn_plaintext` SET TAGS ('mask' = 'PII', 'pii_type' = 'SSN');
```

— the second statement queryable back via `information_schema.column_tags`, and, in this
workspace, immediately live-enforced by the pre-existing `mask` tag policy described in §5.

## Reference

* [Apply tags to Unity Catalog securable objects](https://docs.databricks.com/aws/en/database-objects/tags)
* [Filter sensitive table data using row filters and column masks](https://docs.databricks.com/en/tables/row-and-column-filters.html) — the enforcement mechanism a workspace admin's tag policy may bind to a tag; not something this framework configures.
* [01_control_metadata_schema.md §6](01_control_metadata_schema.md#6-governance_tags-ingestion-and-transformation-flows) — full `governance_tags` field reference.
* [03_engine_execution_flow.md](03_engine_execution_flow.md) — the graph-definition/graph-execution phase split behind §2 above.
