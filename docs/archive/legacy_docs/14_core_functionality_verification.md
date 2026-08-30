# Core Functionality Verification Suite

> See also: [README.md](README.md) for the full Metaflow documentation set.

## Purpose

Exercise the remaining core engine behaviors in Metaflow not already covered by a named
sample pipeline: exhaustive configuration validation, dynamic schema drift handling,
encryption key rotation, governance tag idempotency, onboarding restart/idempotency, and
(pointer to) end-to-end reconciliation verification.

## Responsibilities & relevant tests

| Capability | Artifact | Test |
|---|---|---|
| Configuration validation (exhaustive, real spec) | `test_specs/spec_12_config_validation_negative.json` | `tests/unit/test_config_validation_negative_spec.py` |
| Optional-field defaults (`schema_location`, `auto_ttl`) vs. irreducible fields (`encrypted_columns[].column_name`/`.secret`) | `test_specs/spec_22_optional_fields_df_customer_ingest.json` (irreducible fields), `spec_23_optional_fields_create_live.json` (live CREATE via `optional_fields_verification_job`) | `tests/unit/test_optional_fields_df_customer_ingest_spec.py` |
| SQL parameter substitution validation | (uses `${param}` fixtures inline) | `tests/integration/test_sql_parameter_validation.py` |
| Dynamic schema drift handling | `test_specs/spec_13_schema_drift_handling.json` + `schema_drift_core_test_pipeline` | manual: query `_rescued_data` after seeding batch2 (see below) |
| Encryption key rotation | Unity Catalog secrets `key_rotation_test_key_v1`/`_v2` (`{secret_catalog, secret_schema, secret_key}`) | `tests/integration/test_encryption_key_rotation.py` |
| Governance tag idempotency | `governance/tags.py` | `tests/integration/test_governance_tags.py` |
| Onboarding restart/idempotency | re-onboard `spec_07` twice | `tests/integration/test_idempotent_restart.py` |
| Reconciliation (end-to-end) | `spec_09` | `tests/integration/test_reconciliation.py` (see [07_reconciliation.md](07_reconciliation.md)) |

**Secret reference shape:** `encrypted_columns[].secret_scope`/`.secret_key` (classic
workspace scope) is not used -- every secret reference, including in `spec_22`/`spec_23`
below, is `.secret: {secret_catalog, secret_schema, secret_key}` (a
[Unity Catalog secret](https://docs.databricks.com/aws/en/security/secrets/unity-catalog-secrets),
a three-level namespace, resolved via `dbutils.secrets.get(catalog=, schema=, key=)`). See
[01_control_metadata_schema.md](01_control_metadata_schema.md) §4 and
`crypto/secrets.py`'s module docstring.

## Schema drift: how to observe it

1. `sample_data/sample_schema_drift_events_batch1.json` (3 events, fields `device_id`/
   `event_ts`/`reading`) is seeded automatically.
2. Run `schema_drift_core_test_pipeline` -- `{catalog}.bronze_schema_drift.raw_events` is
   populated, `_rescued_data` is null for every row (no drift yet).
3. Drop `sample_data/sample_schema_drift_events_batch2.json` (adds a `unit` field) into
   `/Volumes/{catalog}/landing/schema_drift_zone/events/` and re-run the pipeline.
4. With `schema_evolution_mode: "rescue"` (not `"addNewColumns"`, which would instead
   fail the stream and require a restart to pick up the new schema -- a deliberately
   different, also-valid drift-handling strategy not exercised here), the new `unit`
   field is captured in `_rescued_data` as a JSON string, and the pipeline **keeps
   running** -- no failure, no dropped data, no manual restart needed.

## Design decisions

* **`rescue`, not `addNewColumns`, for this test.** Both are valid, real
  [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
  schema-evolution strategies (see
  [02_cdc_load_strategies.md](02_cdc_load_strategies.md)'s sibling docs on source
  config) -- `addNewColumns` deliberately fails the stream on a schema change (forcing an
  explicit restart to adopt the new schema, a stronger consistency guarantee some teams
  want), while `rescue` tolerates drift inline. This suite demonstrates the
  no-interruption path since it's the one that can be verified without orchestrating a
  deliberate pipeline failure-then-restart inside an automated test.
* **Governance tag idempotency is tested against real
  [`ALTER TABLE ... SET TAGS`](https://docs.databricks.com/aws/en/data-governance/unity-catalog/tags)
  calls, not a ledger.** The tags-only governance model (`governance/tags.py`) has no
  idempotency ledger to test in isolation -- `governance/idempotency.py` and its
  `governance_applied_log` control table are removed entirely, because tag DDL is
  naturally idempotent (re-applying an identical key/value is a no-op; a changed value
  simply overwrites). `tests/integration/test_governance_tags.py` (replacing the deleted
  `test_governance_idempotency.py`) proves exactly that against a real Delta table:
  multiple tags per column/table apply in one statement, an identical re-application is a
  no-op, a changed value overwrites, and an unsafe/unknown column raises
  `AbacApplicationError` naming the failure without blocking the other tags in the same
  call. Live *enforcement* of a tag (e.g. that `mask: "PII"` actually redacts a column) is
  a separate, workspace-administered UC tag policy outside this framework's scope --
  proven live in [20_crypto_abac_exhaustive_test_suite.md](20_crypto_abac_exhaustive_test_suite.md)
  instead, against this workspace's real, enforced `mask` tag policy.
* **Optional-field defaults stop short of guessing security-relevant values.**
  `spec_22_optional_fields_df_customer_ingest.json` omits `source_config.schema_location`
  (auto-derived as `/Volumes/<target_catalog>/landing/_schemas/<target_table>/`) and
  leaves `target_config.auto_ttl` empty (Auto TTL is opt-in; an incomplete block just
  means TTL isn't applied for that flow -- see
  [01_control_metadata_schema.md](01_control_metadata_schema.md) §4). Its
  `encrypted_columns` entry, by contrast, supplies neither `column_name` nor `secret` and
  both still correctly report as `is required`: this design retired `secret_scope`'s old
  `"security"`-scope convention default along with classic scopes entirely, and a Unity
  Catalog `{secret_catalog, secret_schema, secret_key}` reference has no comparably safe
  universal default -- silently skipping which column to encrypt, or which key to use,
  would leave PII unencrypted with no indication.
* **Config-validation negative test is a real on-disk spec, not an inline dict** --
  `spec_12_config_validation_negative.json` doubles as both a test fixture and a
  reference example of "everything that can go wrong in one spec," matching the worked
  example already in [04_onboarding_validation.md](04_onboarding_validation.md).

## Example usage

```bash
databricks bundle run sample_pipelines_job --profile dev
uv run pytest tests/unit tests/integration --profile dev   # (profile via env, see below)
```

## Relevant tests

All tests listed in the table above; see each linked doc for detail on its own pipeline.
