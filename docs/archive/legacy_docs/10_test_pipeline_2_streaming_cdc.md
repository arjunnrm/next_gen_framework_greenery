# Test Pipeline 2: Zerobus Stream -> Encrypted SCD1/SCD2 (Account Events)

> See also: [Documentation index](README.md).

## Purpose

Prove FlowX handles a **CDC-bus-style streaming source** (Zerobus-landed Delta
table with an explicit operation column), **column-level encryption before
persistence**, and **incremental processing with inserts, updates, and deletes** -- again
entirely through onboarding configuration, using the CDC-delete support added to
`cdc/scd.py` for this pipeline (`register_scd1`/`register_scd2`'s new
`apply_as_deletes` handling -- see [02_cdc_load_strategies.md](02_cdc_load_strategies.md)).

## Responsibilities

* Stream account CDC events from an existing Delta table (simulating a
  [Zerobus](https://docs.databricks.com/en/ingestion/zerobus/index.html) direct-write
  feed) into Bronze, AES-256-GCM-encrypting `ssn`/`email` before the row is ever written
  to disk.
* Derive 5 Silver dimensions, 3 SCD1 + 2 SCD2, all honoring the batch's `op` column
  (`I`/`U`/`D`) to insert, update, or delete rows -- not just append.

## Inputs

* `sample_data/sample_account_events_batch1.csv` (5 accounts, all inserts) /
  `_batch2.csv` (one balance update, one delete, one `contact_channel` update, one new
  insert) -- seeded idempotently (`MERGE` on `event_id`) into
  `{catalog}.bronze_account_ops.zerobus_raw_account_events` by
  `00_seed_sample_data.py::seed_zerobus_event_table_from_csv`.
* `test_specs/spec_08_zerobus_scd_encrypted.json`.
* Classic [secret scope](https://docs.databricks.com/aws/en/security/secrets/)/key
  `security`/`pii_encryption_key` (provisioned manually, same as
  `spec_01`/`spec_02` -- see [05_deployment_guide.md](05_deployment_guide.md)).

## Outputs

* `{catalog}.bronze_account_ops.raw_account_events` -- `ssn`/`email` ciphertext at rest.
* 5 Silver tables: `dim_account_profile_scd1`, `dim_account_balance_scd1`,
  `dim_account_status_scd1`, `dim_account_profile_scd2`, `dim_account_contact_pref_scd2`
  (+ SCD2 reporting tables).

## Configuration

Nothing new beyond what's already documented, **except** one new
`target_config` field pair applied to SCD1/SCD2 (previously snapshot-CDC-only):
`cdc_operation_column` / `cdc_operation_mapping.delete_values`. See
`spec_08_zerobus_scd_encrypted.json`'s `target_config` on every transformation flow.

## Main execution flow

Same shape as Test Pipeline 1 (see
[08_test_pipeline_1_volume_scd.md](08_test_pipeline_1_volume_scd.md)) --
`resources/sample_pipelines_job.yml`'s `onboard_spec_08` ->
`run_account_events_pipeline` -> `apply_governance_account_events`. Run once against
batch1 only, then again after `seed_zerobus_event_table_from_csv` merges in batch2, to
observe the insert/update/delete behavior.

## Design decisions

* **Encrypted columns are never included in `columns_to_check`.** AES-GCM (this
  framework's default, and the mode used here) generates a random IV per call, so
  encrypting the *same* plaintext twice produces *different* ciphertext. If an encrypted
  column were part of SCD2 change-detection, every single pipeline run would look like a
  change even when nothing did -- a silent correctness bug that would only show up as an
  ever-growing, meaningless history table. `dim_account_profile_scd2` tracks only
  `balance`/`status`; `ssn`/`email` ride along as passthrough attributes.
  `test_scd2_encrypted_columns_are_passthrough_not_tracked` in
  `tests/integration/test_pipeline2_streaming_cdc.py` guards this directly.
  Reference: [`aes_encrypt`](https://docs.databricks.com/en/sql/language-manual/functions/aes_encrypt.html).
* **Encryption happens once, at ingestion.** All 5 Silver transformations read the
  already-encrypted Bronze columns as plain passthrough -- no per-target
  decrypt/re-encrypt, since nothing downstream needs the plaintext for this pipeline.
* **`event_id` is the CDC record identifier, `account_id` is the SCD key** -- deliberately
  different columns. Conflating them would make it impossible to tell "a new event about
  an existing account" from "a new account."

## Error handling

`CdcStrategyError` if `cdc_operation_column` is set without `cdc_operation_mapping.delete_values`
(or vice versa) -- enforced by `spec_validator.py`'s new cross-field check, not just at
runtime.

## Extension points

Adding a delete-aware SCD3 target would require extending `cdc/scd.py::register_scd3`
similarly -- not done here since SCD3 is a derived, non-native pivot (see
[02_cdc_load_strategies.md](02_cdc_load_strategies.md)) and delete semantics on a
pivoted current/previous view need their own design discussion, deliberately out of
scope for this pass.

## Example usage

```bash
databricks bundle run sample_pipelines_job --profile dev   # seeds + runs both pipeline 1 and 2
```

## Relevant tests

`tests/integration/test_pipeline2_streaming_cdc.py` -- table existence, encryption at
rest, SCD1 update/delete/insert semantics, SCD2 delete-closes-without-reopening, SCD2
change-only-on-tracked-columns, and encrypted-column/change-detection isolation.
