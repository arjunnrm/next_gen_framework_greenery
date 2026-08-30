# Databricks notebook source
# MAGIC %md
# MAGIC # Seed Metaflow Testing Data
# MAGIC
# MAGIC Provisions the `metaflow` catalog's schemas/volumes and every fixture the 3
# MAGIC `metaflow_testing/*.json` specs need:
# MAGIC
# MAGIC * **001 (EA_usecase)** -- 4 ZIP archives (departments/employees/projects/assignments),
# MAGIC   generated directly on-cluster from the CSV fixtures under
# MAGIC   `sample_data/metaflow_testing/ea_usecase/` (workspace-bundle sync can mangle a
# MAGIC   pre-built `.zip` upload, so this notebook builds the archive bytes itself,
# MAGIC   entirely in-process, and writes them straight to the landing Volume).
# MAGIC * **002/003 (Excalibur_usecase)** -- a Zerobus-style source bus table
# MAGIC   (`zerobus_source_bus`, seeded idempotently via MERGE on `customer_id`, via
# MAGIC   `seed_zerobus_style_table_from_csv` below) and an Auto
# MAGIC   Loader-landed batch CSV (`autoload_batch1.csv`) that deliberately includes 2 customer
# MAGIC   records (`C006`/`C007`) absent from the Zerobus bus -- spec 003's reconciliation flow
# MAGIC   is designed to detect exactly these 2 as missing and append them back into
# MAGIC   `zerobus_source_bus`, so spec 002's next Zerobus stream read picks them up naturally.
# MAGIC
# MAGIC Run once per environment before onboarding `metaflow_testing/001_*.json`,
# MAGIC `002_*.json`, `003_*.json`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets & Path Resolution

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_metaflow_testing_data")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "metaflow_testing")
EA_FIXTURE_DIR = os.path.join(FIXTURE_DIR, "ea_usecase")
EXCALIBUR_FIXTURE_DIR = os.path.join(FIXTURE_DIR, "excalibur_usecase")

for _dir in (EA_FIXTURE_DIR, EXCALIBUR_FIXTURE_DIR):
    if not os.path.isdir(_dir):
        raise FileNotFoundError(f"Expected fixture directory at '{_dir}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved metaflow_testing fixture directories under: %s", FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes

# COMMAND ----------

DATA_SCHEMAS = ["EA_usecase", "bronze_ea", "silver_ea", "egress_ea", "Excalibur_usecase", "bronze_excalibur"]
for _schema in DATA_SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{_schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.EA_usecase.landing_zip")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.EA_usecase._schemas")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.egress_ea.export_zips")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.Excalibur_usecase.landing_autoload")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.Excalibur_usecase._schemas")

logger.info("Provisioned catalog '%s' with %d data schemas + landing/egress volumes.", CATALOG, len(DATA_SCHEMAS))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Scenario 001 -- Generate the 4 EA ZIP Archives On-Cluster

# COMMAND ----------

import csv
import io
import zipfile


def _write_csv_zip_to_volume_from_fixture(zip_path: str, csv_filename: str, fixture_csv_path: str) -> None:
    """Read an already-CSV-shaped fixture file and re-zip it directly into a landing Volume
    -- avoids syncing a pre-built .zip through the bundle (see 01_seed_bt_group_data.py's
    identical note on why that's unreliable)."""
    try:
        with open(fixture_csv_path, "r", encoding="utf-8", newline="") as fixture_file:
            csv_text = fixture_file.read()
        dbutils.fs.mkdirs(os.path.dirname(zip_path))
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(csv_filename, csv_text)
        with open(zip_path, "wb") as destination:
            destination.write(zip_buffer.getvalue())
        logger.info("Generated ZIP fixture directly on cluster: '%s'", zip_path)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Failed to generate ZIP fixture '{zip_path}': {exc}") from exc


EA_INCOMING_ZONE = f"/Volumes/{CATALOG}/EA_usecase/landing_zip/incoming"

for _name in ("departments", "employees", "projects", "assignments"):
    _write_csv_zip_to_volume_from_fixture(
        f"{EA_INCOMING_ZONE}/ea_{_name}.zip", f"{_name}.csv", os.path.join(EA_FIXTURE_DIR, f"{_name}.csv")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Scenario 002 -- Seed the Zerobus-Style Source Bus Table

# COMMAND ----------


def seed_zerobus_style_table_from_csv(csv_path: str, target_table: str, merge_key_column: str) -> None:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Required Zerobus-style fixture missing: '{csv_path}'")
    try:
        target_catalog_schema = ".".join(target_table.split(".")[:-1])
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog_schema}")
        source_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"file:{csv_path}")

        if not spark.catalog.tableExists(target_table):
            source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
            logger.info("Seeded Zerobus-style table '%s' from '%s' (%d rows)", target_table, csv_path, source_df.count())
            return

        from delta.tables import DeltaTable

        # Explicit, source-column-scoped insert (not whenNotMatchedInsertAll()) -- this target
        # table can genuinely have MORE columns than this fixture's CSV by the time this seed
        # step re-runs (e.g. scenario 003's own reconciliation self-healing appends corrected
        # records carrying an extra `updated_at` column into this exact table -- see
        # 003_autoload_recon_append.json's transform_sql -- which Delta's own schema evolution
        # then adds permanently). The ...All() variants require the source to carry every target
        # column, so they fail with DELTA_MERGE_UNRESOLVED_EXPRESSION the moment the target has
        # drifted wider than the fixture -- confirmed live. Scoping to source_df.columns (the
        # same dynamic-column pattern onboarding/metadata_upsert.py already uses for its own
        # MERGE calls) only ever touches the columns this fixture actually provides, leaving any
        # drifted extra target column (e.g. updated_at) untouched on inserted rows.
        target = DeltaTable.forName(spark, target_table)
        merge_columns = {col: f"s.{col}" for col in source_df.columns}
        # INSERT-ONLY on purpose -- do NOT add whenMatchedUpdate() back.
        #
        # This table is the streaming SOURCE for spec 002's `zerobus_bronze` streaming
        # table. A Delta streaming source must be append-only: any commit carrying an
        # update/delete action makes the next read fail outright with
        # DELTA_SOURCE_TABLE_IGNORE_CHANGES, and because a streaming table cannot simply
        # skip past it, the pipeline stays broken until it is fully refreshed.
        #
        # whenMatchedUpdate() re-wrote every already-present row on each re-seed even when
        # the fixture values were byte-identical, so a *second* run of this idempotent
        # seeder was enough to poison the source permanently. Confirmed live on 2026-08-29:
        # SCN-002 failed with "Detected a data update ... in the source table at version 1"
        # naming this table's MERGE commit.
        #
        # Insert-only keeps the seeder just as idempotent -- re-running it inserts nothing
        # for rows that already match on the merge key -- while modelling what a Zerobus
        # event bus actually is: an append-only stream. Changing a fixture VALUE now
        # requires dropping the table rather than editing rows in place, which is the
        # correct trade for a source that must never emit a non-append commit.
        (
            target.alias("t")
            .merge(source_df.alias("s"), f"t.{merge_key_column} = s.{merge_key_column}")
            .whenNotMatchedInsert(values=merge_columns)
            .execute()
        )
        logger.info("Merged Zerobus-style batch '%s' into '%s' (idempotent on '%s')", csv_path, target_table, merge_key_column)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Failed to seed Zerobus-style table '{target_table}' from '{csv_path}': {exc}") from exc


seed_zerobus_style_table_from_csv(
    os.path.join(EXCALIBUR_FIXTURE_DIR, "zerobus_source_bus_batch1.csv"),
    f"{CATALOG}.Excalibur_usecase.zerobus_source_bus",
    "customer_id",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Scenario 003 -- Land the Auto Loader Batch CSV
# MAGIC
# MAGIC Deliberately includes `C006`/`C007`, absent from `zerobus_source_bus` above -- proving
# MAGIC spec 003's reconciliation flow detects them as `MISSING_IN_TARGET` and appends them
# MAGIC back into the Zerobus source bus (not just the Zerobus bronze table), so the *next*
# MAGIC Zerobus stream read of that bus picks them up as if they had arrived normally.

# COMMAND ----------

EXCALIBUR_AUTOLOAD_INCOMING = f"/Volumes/{CATALOG}/Excalibur_usecase/landing_autoload/incoming"
_autoload_fixture = os.path.join(EXCALIBUR_FIXTURE_DIR, "autoload_batch1.csv")
if not os.path.exists(_autoload_fixture):
    raise FileNotFoundError(f"Required Auto Loader fixture missing: '{_autoload_fixture}'")

dbutils.fs.mkdirs(EXCALIBUR_AUTOLOAD_INCOMING)
dbutils.fs.cp(f"file:{_autoload_fixture}", f"{EXCALIBUR_AUTOLOAD_INCOMING}/autoload_batch1.csv")
logger.info("Landed Auto Loader batch fixture at '%s/autoload_batch1.csv'", EXCALIBUR_AUTOLOAD_INCOMING)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC All 3 `metaflow_testing/*.json` specs can now be onboarded and their pipelines run.
# MAGIC Re-running this notebook is safe: the ZIP archives regenerate idempotently (same
# MAGIC content, overwritten), the Zerobus source bus MERGEs (no duplicate rows), and the Auto
# MAGIC Loader batch file is copied fresh each time (Auto Loader itself, not this notebook,
# MAGIC tracks which files it has already ingested via its checkpoint).
