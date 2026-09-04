# Databricks notebook source
# MAGIC %md
# MAGIC # Seed Sample 03 -- Concurrent Multi-Table Load + In-DAG Reconciliation Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `resources/sample_jobs/onboarding/sample_03_multi_table_recon.json`
# MAGIC only. Parameterized by `iteration`; the common seed job
# MAGIC (`resources/sample_jobs/flowx_sample_seed_job.yml`) invokes it once per iteration
# MAGIC (1 -> 2 -> 3, chained) before any sample pipeline runs.
# MAGIC
# MAGIC Each iteration takes a DISTINCT deterministic 40-row slice of `samples.nyctaxi.trips`
# MAGIC (rows 0-39 / 40-79 / 80-119 of a stably-ordered 120-row window; a seed-assigned
# MAGIC `ride_id = RIDE_<iteration>_<n>` is the reconciliation match key, since the trips table
# MAGIC has no natural key) and lands it TWICE -- once for the primary feed, once for the replica
# MAGIC feed -- with **controlled drift** injected into the replica:
# MAGIC
# MAGIC | `iteration` | Replica drift (0-based index within the 40-row slice) | Recon metrics after that update (tables are APPEND, so CUMULATIVE) |
# MAGIC |---|---|---|
# MAGIC | `1` | none -- byte-identical to primary | `matched_count` 40, `missing_in_target_count` 0, `value_drift_count` 0 |
# MAGIC | `2` | `fare_amount + 5.0` where `index % 7 == 0` (6 rows) | `value_drift_count` 6; `missing_in_target_count` 6 (VALUE_DRIFT rows count toward it) |
# MAGIC | `3` | rows where `index % 5 == 0` OMITTED entirely (8 rows) | `value_drift_count` 6 (unchanged); `missing_in_target_count` 14 = 6 drift + 8 missing |
# MAGIC
# MAGIC The recon flow's warn-rules (`missing_in_target_count = 0`, `value_drift_count = 0`)
# MAGIC therefore pass on update 1 and WARN -- by design -- on updates 2 and 3. Falls back to a
# MAGIC small inline literal DataFrame when the `samples` catalog is not shared into this
# MAGIC workspace (the log says which path was taken). Idempotent per iteration.

# COMMAND ----------

import csv
import io
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_sample_03_multi_table_recon")

dbutils.widgets.text("catalog", "flowx", "Target Unity Catalog")
dbutils.widgets.text("iteration", "1", "Which iteration to land (1, 2, or 3)")

CATALOG = dbutils.widgets.get("catalog").strip()
ITERATION = dbutils.widgets.get("iteration").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")
if ITERATION not in ("1", "2", "3"):
    raise ValueError(f"The 'iteration' widget must be '1', '2', or '3' -- got '{ITERATION}'.")

SAMPLE_SCHEMA = "flowx_sample"
LANDING_ROOT = f"/Volumes/{CATALOG}/{SAMPLE_SCHEMA}/landing"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision the Single Sample Schema & Its Volumes
# MAGIC
# MAGIC `observability` is provisioned here too -- this job's `observability_export` task writes
# MAGIC the spec's `DATABRICKS_VOLUME` destination under it.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SAMPLE_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SAMPLE_SCHEMA}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SAMPLE_SCHEMA}.observability")

logger.info("Provisioned %s.%s with the landing/observability volumes.", CATALOG, SAMPLE_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load the Deterministic 120-Row Trip Window (samples Catalog, Inline Fallback)

# COMMAND ----------

_trip_fallback = [
    (
        f"2026-01-{(index % 28) + 1:02d} {(index % 24):02d}:15:00",
        f"2026-01-{(index % 28) + 1:02d} {(index % 24):02d}:45:00",
        round(1.0 + (index % 12) * 0.7, 2),
        round(5.0 + (index % 20) * 2.25, 2),
        str(10000 + (index % 15)),
        str(11200 + (index % 9)),
    )
    for index in range(120)
]

try:
    _collected = spark.sql(
        """
        SELECT CAST(tpep_pickup_datetime AS STRING) AS pickup_ts,
               CAST(tpep_dropoff_datetime AS STRING) AS dropoff_ts,
               CAST(trip_distance AS DOUBLE) AS trip_distance,
               CAST(fare_amount AS DOUBLE) AS fare_amount,
               CAST(pickup_zip AS STRING) AS pickup_zip,
               CAST(dropoff_zip AS STRING) AS dropoff_zip
        FROM samples.nyctaxi.trips
        ORDER BY tpep_pickup_datetime, tpep_dropoff_datetime, fare_amount, trip_distance, pickup_zip, dropoff_zip
        LIMIT 120
        """
    ).collect()
    if not _collected:
        raise ValueError("query returned zero rows")
    base_rows = [row.asDict() for row in _collected]
    logger.info("Loaded %d trip row(s) from the Databricks samples catalog.", len(base_rows))
except Exception as exc:  # noqa: BLE001
    logger.warning(
        "samples catalog unavailable (%s) -- falling back to an inline literal DataFrame (%d row(s)).",
        exc,
        len(_trip_fallback),
    )
    base_rows = [
        row.asDict()
        for row in spark.createDataFrame(
            _trip_fallback,
            "pickup_ts STRING, dropoff_ts STRING, trip_distance DOUBLE, fare_amount DOUBLE, pickup_zip STRING, dropoff_zip STRING",
        ).collect()
    ]

_slice_start = (int(ITERATION) - 1) * 40
_slice = base_rows[_slice_start : _slice_start + 40]

primary_rows = []
for index, row in enumerate(_slice):
    primary_rows.append(
        {
            "ride_id": f"RIDE_{ITERATION}_{index:04d}",
            "pickup_ts": row["pickup_ts"],
            "dropoff_ts": row["dropoff_ts"],
            "trip_distance": round(float(row["trip_distance"]), 2),
            "fare_amount": round(float(row["fare_amount"]), 2),
            "pickup_zip": row["pickup_zip"],
            "dropoff_zip": row["dropoff_zip"],
        }
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Derive the Replica Slice with This Iteration's Controlled Drift

# COMMAND ----------

replica_rows = []
for index, row in enumerate(primary_rows):
    replica_row = dict(row)
    if ITERATION == "2" and index % 7 == 0:
        replica_row["fare_amount"] = round(replica_row["fare_amount"] + 5.0, 2)  # value drift
    if ITERATION == "3" and index % 5 == 0:
        continue  # missing in replica entirely
    replica_rows.append(replica_row)

logger.info(
    "Iteration %s replica drift: %d primary row(s), %d replica row(s), %d value drift(s).",
    ITERATION,
    len(primary_rows),
    len(replica_rows),
    sum(1 for index in range(len(primary_rows)) if ITERATION == "2" and index % 7 == 0),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Land Both Feeds

# COMMAND ----------


def _write_csv(path: str, rows: list) -> None:
    fieldnames = ["ride_id", "pickup_ts", "dropoff_ts", "trip_distance", "fare_amount", "pickup_zip", "dropoff_zip"]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    dbutils.fs.mkdirs(os.path.dirname(path))
    with open(path, "w", encoding="utf-8", newline="") as destination:
        destination.write(buffer.getvalue())
    logger.info("Landed %d row(s) at '%s'.", len(rows), path)


_write_csv(f"{LANDING_ROOT}/sample03_trips_primary/incoming/trips_iter{ITERATION}.csv", primary_rows)
_write_csv(f"{LANDING_ROOT}/sample03_trips_replica/incoming/trips_iter{ITERATION}.csv", replica_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC Iteration landed. See the notebook header for the reconciliation metrics expected after
# MAGIC this iteration's pipeline update. Re-running this notebook with the same `iteration` is
# MAGIC safe: both landing files regenerate idempotently.
