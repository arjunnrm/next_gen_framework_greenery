# ASN.1 Ingestion, DQ, and Quarantine

> See also: [README.md](README.md) for the full Metaflow documentation set.

## Purpose

Prove ASN.1 ingestion (already in Metaflow, see `test_specs/spec_02_*`) combined
with configurable DQ rules and the enriched quarantine metadata added in this pass
(`__framework_pipeline_run_id`, `__framework_record_id`, `__framework_dq_failure_reasons` -- see
`dq/quarantine.py::add_quarantine_columns`), entirely through onboarding configuration.

## Responsibilities

* Decode 5 ASN.1 BER-encoded CDR files against a configured schema
  (`telecom_cdr_v2.asn`, a superset of `telecom_cdr.asn` adding a `recordId`
  field for quarantine traceability).
* Apply 3 quarantine DQ rules: decode success, non-negative call duration, non-empty
  `imsi`.
* Route valid records to `raw_cdr_v2`; invalid records to `raw_cdr_v2_quarantine` with
  full diagnostic metadata.

## Inputs

* `sample_data/asn1_schema/telecom_cdr_v2.asn` (the real ASN.1 module file -- `asn1_pdu_name:
  "CallDetailRecordV2"` derives the Spark output schema from it directly; see
  [01_control_metadata_schema.md §3](01_control_metadata_schema.md)).
* 5 CDR fixtures, generated **directly on the cluster** at seed time (not synced as
  pre-built files -- binary fixtures don't survive Databricks Workspace Files sync
  reliably; see the identical rationale in
  [12_zip_ingestion_pipeline.md](12_zip_ingestion_pipeline.md)):
  * `cdr_001.ber` / `cdr_002.ber` -- valid, no violations.
  * `cdr_003.ber` -- decodes fine, but `callDurationSeconds = -10` (DQ violation).
  * `cdr_004.ber` -- decodes fine, but `imsi = ""` (DQ violation).
  * `cdr_005.ber` -- deliberately malformed, not valid BER at all (decode failure, a
    different failure class than a DQ rule violation).
* `test_specs/spec_11_asn1_dq_quarantine.json`.

## Outputs

* `{catalog}.bronze_telecom_v2.raw_cdr_v2` -- 2 valid records.
* `{catalog}.bronze_telecom_v2.raw_cdr_v2_quarantine` -- 3 records, each carrying:
  * `__framework_source_file_name` (from `attach_technical_metadata`, already existing).
  * `__framework_record_id` (from `record_id_column: "recordId"`).
  * `__framework_dq_failed_rule_ids` / `__framework_dq_failure_reasons` (which rule(s), and why, in plain English).
  * `__framework_quarantine_validated_at` (processing timestamp).
  * `__framework_pipeline_run_id` (best-effort run/update identifier).

## Configuration

Nothing new -- this pipeline is a pure demonstration of existing `asn1` source config,
`dq_rules` with `action: quarantine`, and `target_config.record_id_column` (added in this
pass; see [08_test_pipeline_1_volume_scd.md](08_test_pipeline_1_volume_scd.md) for its
introduction). See [01_control_metadata_schema.md §3](01_control_metadata_schema.md) for
the `asn1` source config reference.

## Main execution flow

Identical to `spec_02`'s (see [03_engine_execution_flow.md](03_engine_execution_flow.md))
-- `read_asn1_source` decodes the binary column into typed fields plus
`_asn1_decode_error`, `add_quarantine_columns` evaluates the 3 quarantine rules per row,
`register_main_and_quarantine_tables` splits the output.

## Design decisions

* **`cdr_005.ber`'s failure is a decode failure, not a DQ rule violation** -- and it's
  still caught, because `dq_asn1_decode_ok` (`_asn1_decode_error IS NULL`) is itself a
  configured quarantine rule. This is deliberate: "validate schema compatibility" and
  "apply DQ rules" are two different concerns in the requirements, and this spec exercises
  both through the *same* quarantine mechanism rather than a separate code path for
  decode failures.
* **`callDurationSeconds IS NULL OR callDurationSeconds >= 0`** (not just `>= 0`) --
  a `NULL` (e.g. from a field that failed to decode independently of the whole-record
  decode error) must not be treated as "failed this rule" by SQL's three-valued logic
  (`NULL >= 0` is `NULL`, not `FALSE`, so without the explicit `IS NULL` branch a null
  value would silently *not* count as a violation -- worth calling out since it's an easy
  mistake).

## Error handling

`Asn1DecodeError` if the schema config itself is malformed; per-record decode failures
are captured inline as `_asn1_decode_error` (never raised), so one malformed file among
five doesn't abort the batch -- exactly `cdr_005.ber`'s role here.

**Real bug found via live deployment, since fixed (see below):** for a
long time, all 5 records ended up in `raw_cdr_v2_quarantine` instead of the 2/3 split
documented above, every one with
`_asn1_decode_error = "FileNotFoundError: ... /Volumes/{{catalog}}/landing/_asn1_schemas/telecom_cdr_v2.asn"`
-- a literal, never-substituted `{{catalog}}` placeholder baked into `module_files` inside
what was then a separate `telecom_cdr_v2.json` schema-config wrapper file. `{{catalog}}`/
`{{env}}` substitution only ever happens on an onboarding spec's own JSON/YAML text
(`onboarding/spec_loader.py`, at *onboarding* time) -- that external JSON file was a
separate artifact read directly at pipeline *execution* time, so it was never in scope of
that substitution at all. The pipeline update itself always reported `SUCCESS` throughout,
because a per-row decode exception routes to quarantine rather than failing the update -- a
100% failure rate was silently indistinguishable from "working as designed" without
actually querying `raw_cdr_v2`'s row count, which
`tests/integration/test_asn1_dq_quarantine.py` would have caught immediately (its
`test_valid_records_land_in_main_table`/`test_three_records_are_quarantined` assertions
fail hard against the buggy behavior) had it ever actually been run. Originally fixed by
substituting `{{catalog}}` in `module_files`, inferred from the JSON wrapper file's own
`/Volumes/<catalog>/...` prefix. See [19_bt_group_test_suite.md](19_bt_group_test_suite.md)
(UC003) for where this was independently rediscovered in a second catalog, and
[05_deployment_guide.md](05_deployment_guide.md)'s `--full-refresh` note for why a plain
re-run of this already-run pipeline kept showing the old failure even after the fix was
deployed -- Auto Loader had already checkpointed the 5 CDR files as processed, so a
code-only fix needed `--full-refresh-all` to actually reprocess them.

**This entire bug class is now eliminated by construction.** The current design
(`asn1_schema_path`/`asn1_codec`/`asn1_pdu_name`, replacing the earlier `asn1_schema_json_path`)
removed the separate external JSON wrapper file entirely -- `asn1_schema_path` is a plain
`source_config` field in the onboarding spec's own JSON/YAML text, so it goes through the
exact same `{{catalog}}`/`{{env}}` substitution as `path`/`schema_location` always have,
before the pipeline ever runs. There is no longer a second, execution-time-only artifact for
a template placeholder to silently slip past. `asn1/decoder.py::load_asn1_schema_config` (and
its manual `{{catalog}}`-inference hack) no longer exists -- see
`asn1/decoder.py::derive_asn1_field_defs`, which also derives the Spark output field list
directly from the real `.asn` module file instead of a hand-authored, drift-prone JSON
`fields` list.

## Extension points

Add a 4th quarantine rule (e.g. `regionCode` allow-list) by adding one `dq_rules` entry
-- no code change.

## Example usage

```bash
databricks bundle run sample_pipelines_job --profile dev   # task: run_asn1_cdr_dq_quarantine_pipeline
```

## Relevant tests

`tests/integration/test_asn1_dq_quarantine.py` -- valid-record count, quarantine-record
count and reasons, and presence of every enriched metadata column.
