# Test Pipeline 1: Volume -> SCD1/SCD2 (Customer 360)

> See also: [Documentation index](README.md).

## Purpose

Prove Metaflow's engine onboards a new pipeline -- 1 Bronze ingestion + 5 Silver dimension
tables, 3 of them SCD1 and 2 SCD2 -- **entirely through onboarding configuration**, with
zero changes to `lakeflow_framework` engine code.

## Responsibilities

* Land customer profile CSV extracts from a [Unity Catalog Volume](https://docs.databricks.com/aws/en/connect/unity-catalog/volumes) (Bronze, `APPEND`).
* Derive 5 Silver dimensions from that one Bronze table:
  * `dim_customer_profile_scd1`, `dim_customer_tier_scd1`, `dim_customer_region_scd1` --
    SCD1 (current-state-only) over different attribute subsets.
  * `dim_customer_profile_scd2`, `dim_customer_contact_scd2` -- SCD2 (full history) with
    independent `columns_to_check` (profile tracks `city`/`country`/`tier`; contact tracks
    only `email`) to prove history-tracking scope is genuinely per-target, not global.

## Inputs

* `sample_data/sample_customer_profile_day1.csv` / `_day2.csv` -- 5 customers on day 1;
  day 2 changes `C001`'s tier (GOLD -> PLATINUM), `C002`'s city, `C003`'s email, and adds a
  new customer `C006` -- covering inserts and updates in one fixture pair.
* `test_specs/spec_07_volume_scd1_scd2_customer_360.json` (and an equivalent `.yaml`,
  proving format parity -- see [09_onboarding_yaml_json.md](09_onboarding_yaml_json.md)).

## Outputs

* `{catalog}.bronze_customer_ops.raw_customer_profile` (+ `_quarantine` sibling for rows
  failing `dq_tier_known_value`).
* The 5 Silver tables above, plus `dim_customer_profile_scd2_current` and
  `dim_customer_contact_scd2_current` -- SCD2 reporting tables aliasing
  [Lakeflow's native](https://docs.databricks.com/aws/en/dlt/cdc)
  `__START_AT`/`__END_AT` tracking columns to `valid_from`/`valid_to`/`is_current` (see
  `cdc/scd.py::register_scd2_reporting_view`). Real `@dlt.table`s, not views -- a view
  registered under this name would never be queryable once the pipeline update finishes.

## Configuration

See `test_specs/spec_07_volume_scd1_scd2_customer_360.json` in full; the only
framework mechanisms exercised are ones already documented in
[01_control_metadata_schema.md](01_control_metadata_schema.md) and
[02_cdc_load_strategies.md](02_cdc_load_strategies.md) -- `source_type: gcs_autoloader`
(backed by [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)),
`cdc_load_strategy: SCD1`/`SCD2`, `primary_keys`/`sequence_by_column`/`columns_to_check`.

## Main execution flow

1. `resources/sample_pipelines_job.yml`: `setup_control_tables` -> `seed_sample_data` ->
   `onboard_spec_07` -> `run_customer_360_pipeline` -> `apply_governance_customer_360`.
2. The pipeline itself (`resources/sample_pipelines.yml`'s
   `customer_360_volume_scd_pipeline`) attaches the exact same
   `notebooks/03_engine/03_lakeflow_declarative_pipeline.py` used by every other group --
   only its `dataflow.group.id` configuration value differs.
3. First run (day1 data only): all 5 targets populated with 5 customers, one version each.
4. Drop `sample_customer_profile_day2.csv` into
   `/Volumes/{catalog}/landing/customer_ops_raw_zone/customer_profile/` and re-run the
   pipeline: SCD1 tables overwrite in place (still 1 row/customer, now 6 with `C006`); SCD2
   tables open a second version for every customer whose tracked columns changed.

## Design decisions

* **One Bronze table, five Silver views** -- not five separate ingestion flows -- keeps
  the landing-zone read/parse cost paid once; each Silver transformation is a plain
  `SELECT` projection over the shared Bronze streaming table.
* **Per-target `columns_to_check`** demonstrates that SCD2 history-tracking scope is a
  target-level configuration choice, not an engine-level one -- the profile and contact
  SCD2 tables version independently off the same source rows.
* **SCD2 reporting table is automatic**, not opt-in -- every `SCD2` target gets one for
  free (see `cdc/scd.py::register_scd2`), so `valid_from`/`valid_to`/`is_current` are
  always available without extra onboarding configuration.

## Error handling

Same framework-wide typed-exception conventions as every other flow --
`FrameworkConfigError` for malformed JSON/config, `CdcStrategyError` for a misconfigured
SCD strategy (e.g. missing `sequence_by_column`), both naming the flow and target table.

## Extension points

Add a 6th dimension (e.g. SCD3 loyalty-tier tracking) by adding one more
`transformation_flow` entry to the spec -- no code change required.

## Example usage

```bash
databricks bundle run sample_pipelines_job --profile dev
# then, after copying sample_customer_profile_day2.csv into the landing volume:
databricks bundle run customer_360_volume_scd_pipeline --profile dev
databricks bundle run sample_pipelines_job --profile dev  # re-applies governance idempotently
```

## Relevant tests

`tests/integration/test_pipeline1_scd.py` -- table existence, SCD1 overwrite-in-place,
SCD1 insert-of-new-key, SCD2 two-version history for a changed customer, SCD2
single-version for an unchanged customer, reporting-view correctness, and independent
history scoping between the two SCD2 targets.
