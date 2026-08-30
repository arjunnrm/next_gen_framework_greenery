# Auto TTL Row Expiration

> See also: [README.md](README.md) — the full Metaflow documentation set.

## Purpose

Prove `target_config.auto_ttl` correctly configures Databricks' row-level Auto TTL
feature -- and document a real, previously-undiscovered bug where Metaflow's TTL
support was **completely non-functional** for every flow that ever configured it, since
its very first commit.

## The bug

[Auto TTL](https://docs.databricks.com/aws/en/tables/operations/auto-ttl) is not a
generic Delta table property the way `delta.logRetentionDuration`/
`delta.deletedFileRetentionDuration` are. It has no `delta.` prefix at all -- its real
properties are `autottl.timestampColumn` and `autottl.expireInDays` -- and for a Lakeflow
streaming table, it can *only* be set via a dedicated `auto_ttl={"timestamp_column": ...,
"expire_in_days": ...}` keyword argument passed directly to the table-creating
`@dlt.table`/`dlt.create_streaming_table` call (`ALTER TABLE ... DELETE ROWS` is
explicitly unsupported for streaming tables -- the only way to set or change it is
pipeline code plus a republish).

This framework's original implementation set
`properties["delta.autoTTL.duration"] = target_config["auto_ttl_duration"]` inside
`build_table_properties`, folded into the generic `table_properties` dict handed to the
`@dlt.table` decorator. This was wrong in every dimension: the property name doesn't
exist (Delta silently ignores an unrecognized custom key -- no error, ever), the value
shape was a duration string rather than an integer day count, and -- the specific gap the
user who requested this test suite correctly suspected -- **the one thing Auto TTL cannot
work without, which column determines a row's age, was never captured in the onboarding
schema at all.** Every flow that ever set `auto_ttl_duration` got silently zero TTL
behavior, with no error to reveal it, for as long as this field has existed.

## Responsibilities

* Land sensor-reading events with a `DATE`/`TIMESTAMP`/`TIMESTAMP_NTZ` column
  (`event_ts`) that Auto TTL uses to determine row age.
* Configure a 7-day expiration window via the new `auto_ttl` object.
* Verify the table's actual `autottl.timestampColumn`/`autottl.expireInDays` properties
  -- not the old, silently-ignored `delta.autoTTL.duration` -- are set correctly.

## Inputs

* `sample_data/sample_sensor_events_ttl.csv` -- 3 rows dated well outside the 7-day
  window relative to this fixture's authoring date (2026-08-26) and 3 rows dated within
  it, so the fixture is realistic even though this suite cannot assert on deletion timing
  (see Design decisions).
* `test_specs/spec_21_auto_ttl_verification.json`.

## Outputs

* `{catalog}.bronze_ttl_verification.raw_sensor_events` -- table properties
  `autottl.timestampColumn = 'event_ts'`, `autottl.expireInDays = '7'`.

## Configuration

```json
{
  "target_config": {
    "auto_ttl": {
      "timestamp_column": "event_ts",
      "expire_in_days": 7
    }
  }
}
```

`timestamp_column` (required) must name a `DATE`/`TIMESTAMP`/`TIMESTAMP_NTZ` column
present on the target. `expire_in_days` (required) is a positive integer. Only
`APPEND`/`TRUNCATE_AND_LOAD` targets are supported today -- the same scope
`partition_columns`/`liquid_clustering_columns` already have (see
`dq/quarantine.py::register_main_and_quarantine_tables`'s non-CDC-dispatch branch);
extending it to SCD/snapshot-CDC targets would need the same `dlt.create_streaming_table`
kwarg threaded through `cdc/scd.py`/`cdc/snapshot.py` too, not attempted here since
neither this bug report nor the fix scope asked for it.

## Prerequisites

Per [Databricks' Auto TTL documentation](https://docs.databricks.com/aws/en/tables/operations/auto-ttl):
Unity Catalog **managed** tables, and
[**Predictive Optimization**](https://docs.databricks.com/aws/en/optimizations/predictive-optimization)
enabled on the metastore/catalog/schema. This is a metastore/account-level setting this
framework does not (and should not) configure itself -- the same reasoning as the classic
secret-scope provisioning step in [05_deployment_guide.md](05_deployment_guide.md) §0. If
Predictive Optimization is off, the table properties will still be set correctly (this
framework's own responsibility, and what this suite actually verifies), but Databricks
will never actually run the deletion.

## Main execution flow

1. `resources/auto_ttl_verification_job.yml`: `setup_control_tables` -> `seed_sample_data`
   -> `onboard_spec_21` -> `run_auto_ttl_verification_pipeline` -> `verify_auto_ttl`.
2. `notebooks/07_verification/07c_verify_auto_ttl.py` asserts the table's real
   `autottl.*` properties, then separately (informationally only) logs how many seeded
   rows are currently older than the 7-day window vs. within it.

## Design decisions

* **Only the *declaration* is verified live, not actual deletion.** Databricks' own
  documentation states "exact deletion timing is not guaranteed" -- Predictive
  Optimization decides when to run the cleanup, asynchronously, on its own schedule. An
  automated test asserting "the old rows are already gone" would be flaky by design (it
  might run before Predictive Optimization's next pass). This suite instead makes a hard,
  deterministic assertion about what the framework itself controls (the table properties
  are set correctly) and reports the row-age snapshot informationally, without failing on
  it either way -- consistent with this repo's other timing-dependent-but-unverifiable
  behaviors (see [06_governance_integration.md](06_governance_integration.md)'s similar
  reasoning for why ABAC binding is a distinct job task).
* **`event_ts`, a business timestamp, not a technical/ingestion-time column** -- Auto TTL
  is about how old the *data* is, not how long ago it was ingested; using
  `__framework_source_file_modification_time` or similar would answer a different question.

## Error handling

`FrameworkConfigError` if `auto_ttl` is configured without `timestamp_column`/
`expire_in_days`, or `expire_in_days` isn't a positive integer -- caught at onboarding
time by `onboarding/spec_validator.py`, not at pipeline-deployment time.

## Extension points

Thread `auto_ttl` through `cdc/scd.py`/`cdc/snapshot.py`'s `dlt.create_streaming_table`
calls to extend support to SCD1/SCD2/SCD3/snapshot-CDC targets -- the same
`build_auto_ttl_kwarg` helper already used here would apply unchanged.

## Example usage

```bash
databricks bundle deploy --profile dev
databricks bundle run auto_ttl_verification_job --profile dev
databricks sql -e "SHOW TBLPROPERTIES poc.bronze_ttl_verification.raw_sensor_events"  # spot-check any time later
```

## Relevant tests

`tests/unit/test_spec_validator.py` -- `auto_ttl` required-field and
strategy-applicability validation. `notebooks/07_verification/07c_verify_auto_ttl.py` --
the live table-property assertions (run as a job task, not `pytest`, since it needs a
real materialized Unity Catalog table).
