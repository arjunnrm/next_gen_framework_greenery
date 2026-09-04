# Databricks notebook source
# MAGIC %md
# MAGIC # Provision the `flowx_sample` Schema and Its Volumes (Sample Suite Root Task)
# MAGIC
# MAGIC The single root task of `resources/sample_jobs/flowx_sample_seed_job.yml`. Creates the
# MAGIC one schema and the four Volumes the whole reference suite shares, **once, serially**,
# MAGIC before the six per-sample seed chains fan out in parallel.
# MAGIC
# MAGIC ## Why this task exists at all
# MAGIC
# MAGIC Every `04_seed_sample_*` notebook already issues its own
# MAGIC `CREATE SCHEMA/VOLUME IF NOT EXISTS` -- they must, so each stays runnable on its own. But
# MAGIC `IF NOT EXISTS` is idempotent in *intent*, not *atomic*: Unity Catalog lets two
# MAGIC concurrent creates of the same object race, and the loser fails outright (the same class
# MAGIC of failure as the concurrent `CREATE OR REPLACE FUNCTION` in
# MAGIC `01_setup_control_tables.py`, recorded as pitfall 7 in
# MAGIC `agent_skills/reference/common_pitfalls.md`). Six seed chains starting at the same instant
# MAGIC is exactly that race. Creating everything here first closes the window: by the time the
# MAGIC per-sample seeds run, every `IF NOT EXISTS` is a no-op that cannot lose a race.
# MAGIC
# MAGIC ## The four Volumes
# MAGIC
# MAGIC | Volume | Holds |
# MAGIC |---|---|
# MAGIC | `landing` | every sample's incoming/extracted files, ZIP archives, ASN.1 modules, `_schemas` checkpoints |
# MAGIC | `exports` | Sample 04's password-protected ZIP egress |
# MAGIC | `observability` | Samples 01/03's `DATABRICKS_VOLUME` observability destination |
# MAGIC | `sample_configs` | the one Volume every sample job's onboarding spec JSON is published into (see `notebooks/09_sample_reference/09a_store_sample_config.py`) |
# MAGIC
# MAGIC Idempotent and safe to re-run at any time.

# COMMAND ----------

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("provision_sample_schema")

dbutils.widgets.text("catalog", "flowx", "Target Unity Catalog")

CATALOG = dbutils.widgets.get("catalog").strip()
if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

SAMPLE_SCHEMA = "flowx_sample"
VOLUMES = ("landing", "exports", "observability", "sample_configs")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SAMPLE_SCHEMA}")
logger.info("Provisioned schema %s.%s.", CATALOG, SAMPLE_SCHEMA)

for _volume in VOLUMES:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SAMPLE_SCHEMA}.{_volume}")
    logger.info("Provisioned volume %s.%s.%s.", CATALOG, SAMPLE_SCHEMA, _volume)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC The per-sample seed chains can now fan out in parallel with no create race.

# COMMAND ----------

logger.info(
    "Sample suite storage ready: %s.%s + volumes %s.", CATALOG, SAMPLE_SCHEMA, ", ".join(VOLUMES)
)
