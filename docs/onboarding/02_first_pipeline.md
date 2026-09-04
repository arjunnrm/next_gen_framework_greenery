# 2 · Your first pipeline

Ingest a CSV from a Volume into a Bronze streaming table. Nothing optional, nothing clever.

## Land a file

```bash
databricks fs cp orders.csv \
  dbfs:/Volumes/flowx/framework/landing/orders/incoming/orders.csv --profile <profile>
```

## The smallest useful spec

```json
{
  "dataflow_group_id": "dfg_orders_demo",
  "ingestion_flows": [
    {
      "dataflow_id": "df_orders_raw",
      "source_type": "autoloader",
      "source_config": {
        "path": "/Volumes/flowx/framework/landing/orders/incoming/",
        "format": "csv",
        "schema_location": "/Volumes/flowx/framework/landing/_schemas/orders/",
        "reader_options": { "header": "true", "cloudFiles.inferColumnTypes": "true" },
        "capture_technical_metadata": true
      },
      "target_catalog": "flowx",
      "target_schema": "bronze",
      "target_table": "orders_raw",
      "target_type": "streaming_table",
      "target_config": { "cdc_load_strategy": "APPEND" }
    }
  ]
}
```

Four things are doing real work here:

- **`path`** ends with `/`. A path that looks like a file makes Auto Loader watch the parent directory.
- **`schema_location`** is per-flow and outside the ingested directory. Share it between flows and
  both schemas corrupt.
- **`reader_options`** — without `cloudFiles.inferColumnTypes`, every CSV column arrives as a string.
- **`capture_technical_metadata`** adds the file-provenance columns and the ingestion timestamp that
  CDC strategies later default to.

## Onboard and run

```bash
databricks bundle deploy -t dev_flowx
databricks bundle run onboarding_job -t dev_flowx \
  --params spec_path=/Volumes/flowx/framework/onboarding_specs/orders.json
```

Then start the pipeline. Onboarding wrote the control-table rows; the pipeline compiles its graph
from them.

## Check it worked

```sql
SELECT * FROM flowx.bronze.orders_raw LIMIT 10;

-- the provenance columns capture_technical_metadata added
SELECT __framework_source_file_name, __framework_ingestion_timestamp_utc
FROM flowx.bronze.orders_raw LIMIT 5;
```

!!! danger "Reports SUCCESS but zero rows?"
    Almost always the watched directory is not where the files are. This is the single most common
    silent misconfiguration — see [Known limitations](../13_known_limitations_and_gotchas.md).

## Next

- Add data-quality rules → [Data quality & governance](../04_data_quality_and_governance.md)
- Turn it into SCD2 → [Transformation & CDC](../03_transformation_and_cdc.md)
- Stop hand-writing JSON → [Spec Builder](03_spec_builder_app.md)
