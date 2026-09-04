# Databricks notebook source
# MAGIC %md
# MAGIC # Seed Sample 02 -- Glob-Filtered ZIP Ingestion Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `resources/sample_jobs/onboarding/sample_02_zip_ingestion.json` only.
# MAGIC Parameterized by `iteration`; the common seed job
# MAGIC (`resources/sample_jobs/flowx_sample_seed_job.yml`) invokes it once per iteration
# MAGIC (1 -> 2 -> 3, chained) before any sample pipeline runs.
# MAGIC
# MAGIC Each iteration builds ONE ZIP archive **in-process** (`zipfile` over an in-memory
# MAGIC buffer -- never a pre-built `.zip` fixture, workspace-bundle sync can mangle binary
# MAGIC uploads) from a DISTINCT 30-row slice of `samples.tpch.orders` (rows 0-29 / 30-59 / 60-89
# MAGIC of a deterministic 90-row window), plus 3 deliberately broken rows per iteration with a
# MAGIC NULL `customer_id` -- those are what the spec's quarantine rule routes into
# MAGIC `sample_zip_orders_raw_quarantine`. Falls back to a small inline literal DataFrame when
# MAGIC the `samples` catalog is not shared into this workspace (the log says which path was
# MAGIC taken).
# MAGIC
# MAGIC The archive name (`sample_orders_iter<N>.zip`) matches the spec's
# MAGIC `zip_file_pattern: "sample_orders_*.zip"`. Idempotent: re-running an iteration overwrites
# MAGIC the same archive with the same deterministic bytes.

# COMMAND ----------

import csv
import io
import logging
import zipfile

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_sample_02_zip_ingestion")

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
ORDER_DATE = {"1": "2026-09-01", "2": "2026-09-02", "3": "2026-09-03"}[ITERATION]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision the Single Sample Schema & Landing Volume

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SAMPLE_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SAMPLE_SCHEMA}.landing")

logger.info("Provisioned %s.%s with the landing volume.", CATALOG, SAMPLE_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load the Deterministic 90-Row Order Window (samples Catalog, Inline Fallback)

# COMMAND ----------

_STATUSES = ["OPENED", "PROCESSING", "FULFILLED"]
_order_fallback = [
    (1000 + index, (index % 40) + 1, _STATUSES[index % 3], round(1500.0 + index * 987.3, 2), f"{(index % 5) + 1}-MEDIUM")
    for index in range(90)
]

try:
    _collected = spark.sql(
        """
        SELECT o_orderkey AS order_id, o_custkey AS customer_id,
               CAST(o_totalprice AS DOUBLE) AS total_price, o_orderpriority AS order_priority
        FROM samples.tpch.orders
        ORDER BY o_orderkey
        LIMIT 90
        """
    ).collect()
    if not _collected:
        raise ValueError("query returned zero rows")
    # samples.tpch.orders' own o_orderstatus codes (O/F/P) are remapped to this sample's
    # human-readable statuses deterministically by row position, matching the spec's
    # order_status_known drop-rule vocabulary.
    base_rows = [
        {
            "order_id": int(row["order_id"]),
            "customer_id": int(row["customer_id"]),
            "order_status": _STATUSES[index % 3],
            "total_price": round(float(row["total_price"]), 2),
            "order_priority": row["order_priority"],
        }
        for index, row in enumerate(_collected)
    ]
    logger.info("Loaded %d order row(s) from the Databricks samples catalog.", len(base_rows))
except Exception as exc:  # noqa: BLE001
    logger.warning(
        "samples catalog unavailable (%s) -- falling back to an inline literal DataFrame (%d row(s)).",
        exc,
        len(_order_fallback),
    )
    base_rows = [
        row.asDict()
        for row in spark.createDataFrame(
            _order_fallback,
            "order_id LONG, customer_id LONG, order_status STRING, total_price DOUBLE, order_priority STRING",
        ).collect()
    ]

_slice_start = (int(ITERATION) - 1) * 30
iteration_rows = [dict(row, order_date=ORDER_DATE) for row in base_rows[_slice_start : _slice_start + 30]]

# 3 deliberately broken rows per iteration: order_id populated (it is the quarantine table's
# record_id_column), customer_id EMPTY -> NULL after cloudFiles.inferColumnTypes -> routed to
# sample_zip_orders_raw_quarantine by the customer_id_present quarantine rule.
for bad_index in range(3):
    iteration_rows.append(
        {
            "order_id": 900000 + int(ITERATION) * 10 + bad_index,
            "customer_id": "",
            "order_status": "OPENED",
            "total_price": 10.0 + bad_index,
            "order_priority": "5-LOW",
            "order_date": ORDER_DATE,
        }
    )

logger.info(
    "Prepared iteration %s: %d clean row(s) + 3 quarantine-bound row(s).", ITERATION, len(iteration_rows) - 3
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build the ZIP In-Process and Land It

# COMMAND ----------

_fieldnames = ["order_id", "customer_id", "order_status", "total_price", "order_priority", "order_date"]
_csv_buffer = io.StringIO()
_writer = csv.DictWriter(_csv_buffer, fieldnames=_fieldnames)
_writer.writeheader()
for _row in iteration_rows:
    _writer.writerow(_row)

_zip_buffer = io.BytesIO()
with zipfile.ZipFile(_zip_buffer, "w", compression=zipfile.ZIP_DEFLATED) as _archive:
    _archive.writestr(f"orders_iter{ITERATION}.csv", _csv_buffer.getvalue())

_incoming_dir = f"{LANDING_ROOT}/sample02_zip/incoming"
_zip_path = f"{_incoming_dir}/sample_orders_iter{ITERATION}.zip"
dbutils.fs.mkdirs(_incoming_dir)
with open(_zip_path, "wb") as _destination:
    _destination.write(_zip_buffer.getvalue())

logger.info("Landed ZIP archive '%s' (%d row(s), %d bytes).", _zip_path, len(iteration_rows), len(_zip_buffer.getvalue()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC Iteration landed. Expected after this iteration's pipeline update: 30 more clean rows in
# MAGIC `sample_zip_orders_raw` and 3 more quarantined rows in `sample_zip_orders_raw_quarantine`.
# MAGIC Re-running this notebook with the same `iteration` is safe (same bytes, overwritten).
