# Exhaustive Encryption, Decryption, and Governance Tags Test Suite

> See also: [README.md](README.md) — the full FlowX documentation set.

## Purpose

A dedicated, live, production-grade test job proving every encryption/decryption mode
FlowX supports, plus its tags-only governance model -- not just the golden path
already covered by `spec_08`'s encryption columns, but the real gaps those leave open:
CBC/ECB modes (never exercised anywhere else in this repo), the `decrypted_columns` code
path (never exercised at all before this suite), real `ALTER TABLE ... SET TAGS`
application against a real Unity Catalog table proven against this workspace's actual live
tag-enforcement policy (not just a unit-level check that the DDL ran), and the full set of
documented-but-never-tested error paths (wrong key, bad key length, unsupported mode,
unsafe identifiers, mode mismatch, unsafe/unknown tag column).

**A naming note:** this suite predates the tags-only governance model documented in
[01_control_metadata_schema.md](01_control_metadata_schema.md) §6. `abac_config`
(`row_filters`/`column_masks`, UC function bindings the framework itself created and
bound) is gone; `governance_tags` (`column_tags`/`table_tags`, plain key-value tags the
framework applies but never binds to an enforcement function) replaced it everywhere,
including in `spec_20_crypto_abac_exhaustive.json` and this suite's own verification
notebook. The suite kept its historical "abac" name -- job/pipeline resources
(`crypto_abac_exhaustive_test_job`/`_pipeline`), the `apply_abac` widget on
`04_apply_governance_and_egress.py`, and the `AbacApplicationError` exception class
`governance/tags.py` still raises are all older names now carried by the current
tag-application code, not evidence that ABAC policy binding still happens anywhere in
this framework. Read every "ABAC"/"abac" reference below (including in file/resource
names) in that light.

## Responsibilities

* Encrypt one PII field four ways at ingestion (GCM, CBC, ECB, and a distinct
  `output_column`-override case), then decrypt all four at a downstream transformation --
  the one config path (`decrypted_columns`) nothing else in this repo exercises.
* Apply a real `table_tags.row_filter` and a real `column_tags[].tags.mask`/`.pii_type`
  pair via `governance_tags`, and prove -- against this workspace's actual, live, enforced
  Unity Catalog tag policy on the `mask` key -- that the tag the framework applied is
  genuinely acted on, not merely written to `information_schema`.
* Prove governance tag idempotency and changed-value overwrite against real `ALTER TABLE
  ... SET TAGS` calls (there is no idempotency ledger to test in isolation -- see Design
  decisions).
* Exercise every documented-but-untested encryption/tag-application error path via direct
  function calls, isolated from the DLT graph so a deliberate failure never blocks the
  pipeline.

## Inputs

* `sample_data/sample_customer_pii_encryption.csv` -- 4 accounts: `ACC001`/`ACC002` share
  `region_code = US-EAST` and an identical plaintext SSN in 3 of 4 PII columns (to prove
  determinism differences across GCM/CBC/ECB on the same value), `ACC003`/`ACC004` use
  `region_code = EU-WEST` with distinct values.
* `test_specs/spec_20_crypto_abac_exhaustive.json`.
* `notebooks/04_governance/04b_provision_governance_udfs_and_entitlements.py` -- provisions
  a `user_entitlements` table and two SQL functions as a **reference example** of what a
  workspace admin's own tag-policy enforcement function could look like (see Design
  decisions); not bound to anything by this framework.
* [Unity Catalog secrets](https://docs.databricks.com/aws/en/security/secrets/)
  `{secret_catalog, secret_schema, secret_key}` = `{catalog,
  security, pii_encryption_key}` (shared with every other encryption spec) plus two
  test-only secrets in the same schema, `pii_encryption_key_wrong_test` and
  `pii_encryption_key_badlength` -- see [05_deployment_guide.md](05_deployment_guide.md)
  §0 for provisioning, and `crypto/secrets.py` for why every secret reference in this
  framework is a three-level UC secret, never a classic workspace scope.

## Outputs

* `{catalog}.bronze_crypto_abac.raw_customer_pii` -- `ssn_for_gcm`/`ssn_for_cbc`/
  `ssn_for_ecb` (ciphertext, in-place overwrite of their source columns) and
  `ssn_override_ciphertext` (a *new* column, deliberately alongside its still-plaintext
  source `ssn_for_override` -- the one column in this suite that keeps plaintext at rest,
  entirely on purpose, to prove the `output_column`-override coexistence case; see Design
  decisions).
* `{catalog}.silver_crypto_abac.customer_pii_authorized` -- `ssn_gcm_plain`/
  `ssn_cbc_plain`/`ssn_ecb_plain`/`ssn_plaintext` (all decrypted via `decrypted_columns`),
  with `governance_tags` applying a `row_filter` table tag and a `mask`/`pii_type` column
  tag pair on `ssn_plaintext` -- **declarative tags only; this framework never binds
  anything to them.** This workspace happens to have its own live tag-enforcement policy
  on the `mask` key (see below), which is what actually makes `ssn_plaintext` come back
  redacted when queried.
* `{catalog}.governance.user_entitlements` / `fn_crypto_abac_region_filter` /
  `fn_crypto_abac_region_filter_v2` / `fn_crypto_abac_mask_ssn` -- provisioned by this
  suite's `04b` notebook as a reference example, independent of `spec_20`'s pipeline run;
  **not** referenced by `spec_20`'s `governance_tags` or bound to
  `customer_pii_authorized` by anything this framework does.
* No `{catalog}.config.governance_applied_log` -- there is no governance idempotency
  ledger at all (`governance/idempotency.py` was removed; see Design decisions).

## Configuration

See `test_specs/spec_20_crypto_abac_exhaustive.json` in full -- every field used
(`encrypted_columns`, `decrypted_columns`, `governance_tags.table_tags`/`.column_tags`) is
already documented in [01_control_metadata_schema.md](01_control_metadata_schema.md)
§4/§6/§7; nothing new was added to the onboarding schema for this suite. `governance_tags`
maps onto
[Unity Catalog tags](https://docs.databricks.com/aws/en/database-objects/tags), applied
via `ALTER TABLE ... SET TAGS`. The governance half of the transformation flow:

```json
"governance_tags": {
  "table_tags": {"row_filter": "region_restricted"},
  "column_tags": [
    {"column": "ssn_plaintext", "tags": {"mask": "PII", "pii_type": "SSN"}}
  ]
}
```

## Main execution flow

1. `resources/crypto_abac_exhaustive_test_job.yml`: `setup_control_tables` ->
   `seed_sample_data` -> `{onboard_spec_20, provision_governance_udfs}` (parallel) ->
   `run_crypto_abac_pipeline` -> `apply_governance_and_egress` (applies `governance_tags`
   for the first time -- no egress step runs here any more, see
   [15_engine_refactor.md](15_engine_refactor.md)'s Phase 7) ->
   `verify_crypto_abac_exhaustive`.
2. `notebooks/04_governance/04b_provision_governance_udfs_and_entitlements.py` creates
   `{catalog}.governance.user_entitlements` (seeded with one row for `current_user()`,
   `allowed_region = 'US-EAST'`, `can_view_pii = false`) and the two SQL functions, both
   genuinely attribute-based (they look up the querying principal live, per query, rather
   than encoding a static predicate) -- entirely independent of `run_crypto_abac_pipeline`
   and `apply_governance_and_egress`, kept only as a worked reference.
3. `notebooks/07_verification/07_verify_crypto_abac_exhaustive.py` runs ~30 checks across
   6 sections (golden-path crypto, error-path crypto, governance tags applied, tag
   idempotency, changed-tag-value overwrite, governance negative paths) and raises
   `AssertionError` naming every failure if any check failed -- see the file for the full
   list.

## Two real bugs found via live deployment, on the very first attempt to run this suite

* **Encryption/decryption silently corrupted every ciphertext byte, everywhere in this
  repo, from day one.** See `crypto/column_crypto.py`'s and `crypto/secrets.py`'s module
  docstrings for the full root-cause writeup: embedding a literal `secret(...)` SQL call
  inside a Lakeflow-materialized expression (or even inside its own standalone resolution
  query, once that call also moved outside the write) triggers Databricks' credential
  redaction machinery, which mutates the result. Confirmed live that this had *already*
  been silently corrupting `spec_08`'s `ssn`/`email` columns the entire time -- the only
  prior test (`test_bronze_ssn_and_email_are_encrypted_not_plaintext`) checked "not
  plaintext," never an actual round trip, so the corruption was invisible until this
  suite's `gcm_roundtrip_bronze_to_silver` scenario decrypted a value for the first time
  anywhere in the project's test history. Fixed by resolving the secret via
  `dbutils.secrets.get()` instead of SQL `secret()` at all, then passing it into PySpark's
  native `functions.aes_encrypt`/`aes_decrypt` as a `Column` literal.
* **Column masks failed 100% of the time the live `SET MASK` DDL path was exercised.**
  `governance/abac.py` defaulted `column_masks[].using_columns` to `[column]` (the masked
  column, again) when omitted -- but Unity Catalog's `SET MASK` DDL already passes the
  masked column's own value as the mask function's implicit first parameter, so this
  always supplied one argument too many, failing every single-parameter mask function
  live with `WRONG_NUM_ARGS.WITHOUT_SUGGESTION`. This exact default was also baked into
  `spec_02`'s and the onboarding template's own `column_masks` examples, unnoticed because
  nothing had ever actually bound a live column mask before this suite's
  `column_mask_binding_and_ledger_write` scenario. Fixed at the time by defaulting to no
  additional columns, and by rejecting a `column_masks[].using_columns` entry that
  repeated the masked column at onboarding time. **This bug and its fix are now historical
  only** -- `governance/abac.py`, `abac_config`, and every `SET MASK`/`SET ROW FILTER`
  binding call the framework itself issued are removed entirely under the v2 tags-only
  model (`governance/tags.py`), so there is no `using_columns`/binding-arity code path
  left in this framework for the bug to recur in. Kept here as project history, not a live
  risk.

## Design decisions

* **The framework proves tag *application*, live enforcement is proven separately via this
  workspace's own tag policy.** The tags-only model means `apply_governance_tags` only
  ever issues `ALTER TABLE ... SET TAGS`/`... ALTER COLUMN ... SET TAGS` -- it does not,
  and cannot, bind a row filter or column mask function to anything (see
  `governance/tags.py`'s module docstring). This workspace happens to have a real, live,
  enforced Unity Catalog tag policy restricting the `mask` tag key to values `[PII, cost]`
  -- confirmed live: reading `ssn_plaintext` directly off `customer_pii_authorized` after
  `governance_tags` applies `mask: "PII"` to it returns `"***-**-1111"`/`"***-**-2222"`,
  not the raw decrypted value (Section C's
  `silver_ssn_plaintext_is_actively_masked_by_a_live_uc_tag_policy` check). That's a real,
  end-to-end proof that this framework's tag output is genuinely acted on in this
  workspace -- but it's this workspace's own tag-policy configuration doing the enforcing,
  not code in this repo, and it would not hold in a workspace with no such policy bound to
  the `mask` key. The `user_entitlements` table and `fn_crypto_abac_region_filter`(`_v2`)/
  `fn_crypto_abac_mask_ssn` functions `04b_provision_governance_udfs_and_entitlements.py`
  provisions are kept purely as a **reference example** of what a genuinely
  attribute-based (`current_user()`-driven) enforcement function looks like, for a
  workspace admin who wants to bind a tag policy that calls them -- the verification
  notebook does not bind, call, or otherwise exercise them at all; they exist in the
  catalog but are inert as far as this suite's own checks are concerned.
* **Negative/error scenarios run as direct Python calls, outside the DLT graph.** A live
  Lakeflow Declarative Pipeline aborts its *entire* update on an unhandled exception in any
  one flow -- there's no way to host an intentionally-failing scenario inside the golden
  DLT graph without blocking every other flow (and the downstream governance/verification
  tasks) in the same update. Every wrong-key/bad-length/unsupported-mode/unsafe-identifier
  scenario instead calls `crypto/column_crypto.py`'s functions directly against a
  throwaway single-row DataFrame in the verification notebook -- still real, live code
  execution against the real deployed package, just outside the pipeline's own graph.
  Governance negative paths (unsafe column identifier, unknown column) likewise call
  `apply_governance_tags` directly against an isolated
  `{catalog}.crypto_abac_scratch.negative_test_table`, never the golden-path
  `customer_pii_authorized` table, so a deliberate failure there can never affect the real
  applied tags.
* **`output_column` override is the one deliberate exception to "never leave plaintext at
  rest."** Proving `output_column != column_name` coexistence requires seeing the
  original plaintext source column and the new ciphertext column side by side at least
  once -- `ssn_for_override` (plaintext) stays in `raw_customer_pii` for exactly this
  reason. The silver transformation's `SELECT` deliberately never includes
  `ssn_for_override`, so this is the only layer where it's visible, and it's clearly
  called out as test-only, not a pattern to copy for real PII columns. Downstream,
  `ssn_override_ciphertext` decrypts to `ssn_plaintext` -- the one column this workspace's
  live tag policy actually masks, so its round-trip correctness is proven indirectly
  (Section C's live-masking check), not by asserting a raw plaintext match.
* **Changed-tag-value overwrite is proven directly.** The verification notebook's
  scenario changes `table_tags.row_filter`'s own *value* (`"region_restricted"` ->
  `"region_restricted_v2"`) via a second `apply_governance_tags` call and confirms the new
  value overwrites the old one, then restores the original value so a later idempotency
  check against the live table stays consistent with what `spec_20`'s own
  `governance_tags` declares. `fn_crypto_abac_region_filter_v2` is still provisioned by
  `04b` (kept as a second, identically-shaped reference function) but, per the point
  above, is not actually exercised by any check in the current suite -- provisioned, not
  consumed.
* **CBC determinism is measured, not assumed.** No prior test or doc in this repo actually
  pins down whether Databricks' `aes_encrypt(..., 'CBC')` uses a random IV (like GCM) or is
  deterministic (like ECB) -- the verification notebook logs the empirical result as an
  informational check rather than asserting a specific outcome either way.
* **No idempotency ledger to test.** Tag DDL has no rebinding hazard the way ABAC-style
  function binding would -- `SET TAGS` is naturally idempotent, and a changed value simply
  overwrites, so there's no ledger (`governance/idempotency.py`/`governance_applied_log`,
  both removed) to test in isolation. Idempotency and changed-value overwrite are proven
  here directly against live `ALTER TABLE` calls (Sections D/E), with no ledger
  read/write in between.

## Error handling

Every crypto function already raises `CryptoError` (wrapping a missing config key,
unsupported mode, unknown column, or unsafe identifier). `governance/tags.py`'s
`apply_governance_tags` raises `AbacApplicationError` (best-effort per column/table,
collected across all failures and raised once at the end) on an unsafe or unknown
column/table identifier -- the same exception class the framework's earlier ABAC-binding
code raised, carried forward onto the current tag-application failures rather than
renamed (see this doc's opening naming note). There is no `FrameworkConfigError` from an
idempotency ledger read/write, since there is no ledger. This suite is the first place in
the repo that actually exercises each of the crypto and tag-application error paths with a
live assertion instead of a docstring claim.

## Extension points

Add a 3rd column-tag or table-tag scenario (e.g. a second masked column) by extending
`governance_tags` on `spec_20`'s transformation flow -- no engine change required;
`apply_governance_tags` already supports an arbitrary number of `column_tags`/`table_tags`
entries per flow, applied in one `SET TAGS` statement each. Provisioning an additional
reference enforcement function in `04b_provision_governance_udfs_and_entitlements.py` is
optional and, per the current design, purely illustrative -- this framework's own checks
never call it.

## Example usage

```bash
databricks bundle deploy --profile dev
databricks bundle run crypto_abac_exhaustive_test_job --profile dev
```

## Relevant tests

This suite's own live proof is `notebooks/07_verification/07_verify_crypto_abac_exhaustive.py`
-- run as a job task, not `pytest`, since Section C's live-masking check depends on this
specific workspace's own tag policy already being bound to the `mask` key: a `pytest`
fixture can prove `governance/tags.py` issued the correct `ALTER TABLE ... SET TAGS` DDL,
but it cannot prove a workspace-external tag policy actually redacts the result, and
asserting that against a workspace where no such policy exists would simply fail for a
reason outside this repo's control.

`governance/tags.py`'s own DDL-application mechanics -- multiple tags per column/table,
idempotent re-application, changed-value overwrite, `AbacApplicationError` on an
unsafe/unknown column -- are covered independently and repeatably by
`tests/integration/test_governance_tags.py` (`pytest`-run, no live tag-policy dependency).
This job's Sections D/E duplicate those same properties end-to-end against `spec_20`'s own
table, as part of the one live suite pass.
