# 1 · Prerequisites

Roughly fifteen minutes, once per environment.

## What you need

| Requirement | Why |
|---|---|
| A Unity Catalog catalog | Every target table is `catalog.schema.table` |
| A landing Volume | Auto Loader reads files from a Volume path |
| A schema for control tables | The framework's eight control tables live here |
| A serverless or classic compute policy | Lakeflow pipelines need somewhere to run |
| Databricks CLI >= 0.230 | `databricks bundle deploy` |

## Create the objects

```sql
CREATE CATALOG IF NOT EXISTS flowx;
CREATE SCHEMA  IF NOT EXISTS flowx.framework;   -- control tables
CREATE SCHEMA  IF NOT EXISTS flowx.bronze;      -- ingestion targets
CREATE SCHEMA  IF NOT EXISTS flowx.silver;      -- transformation targets

CREATE VOLUME IF NOT EXISTS flowx.framework.landing;
CREATE VOLUME IF NOT EXISTS flowx.framework.wheels;
CREATE VOLUME IF NOT EXISTS flowx.framework.onboarding_specs;
```

## Grants

The pipeline's run-as identity needs these. Missing grants are the most common first-run failure,
and they surface at pipeline runtime rather than at onboarding.

```sql
GRANT USE CATALOG ON CATALOG flowx TO `<principal>`;
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA flowx.bronze TO `<principal>`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA flowx.framework TO `<principal>`;
GRANT READ VOLUME ON VOLUME flowx.framework.landing TO `<principal>`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME flowx.framework.onboarding_specs TO `<principal>`;
-- only if you use governance_tags:
GRANT APPLY TAG ON SCHEMA flowx.bronze TO `<principal>`;
```

!!! tip "Check before you build"
    The Spec Builder runs a non-destructive permission preflight and shows the result in the save
    panel. If it is unhappy there, onboarding will be unhappy too.

## Verify

```bash
databricks fs ls dbfs:/Volumes/flowx/framework/landing --profile <profile>
databricks bundle validate -t dev_flowx
```

Both succeeding means you are ready for [your first pipeline](02_first_pipeline.md).
