# Databricks notebook source
# MAGIC %md
# MAGIC # Seed Sample 04 -- Encrypted ZIP CSV Export Egress Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `metaflow_testing/samples/sample_04_export_encrypt_zip.json`
# MAGIC only. Parameterized by `iteration`; the common seed job
# MAGIC (`resources/sample_jobs/metaflow_sample_seed_job.yml`) invokes it once per iteration
# MAGIC (1 -> 2 -> 3, chained) before any sample pipeline runs.
# MAGIC
# MAGIC Lands plain-CSV order/customer slices from `samples.tpch.orders` / `samples.tpch.customer`
# MAGIC (a DISTINCT deterministic 50-row order slice per iteration; the customer dimension grows
# MAGIC by a few new keys per iteration) -- the ENCRYPTION happens on the way OUT, inside the
# MAGIC pipeline's two `pgp_zip` sinks, whose `post_export_archive.secret` resolves the UC secret
# MAGIC `<catalog>.metaflow_sample.sample_zip_passkey` at export time. Falls back to small inline
# MAGIC literal DataFrames when the `samples` catalog is not shared into this workspace (the log
# MAGIC says which path was taken).
# MAGIC
# MAGIC **Fail-fast secret preflight:** this seed needs no passphrase itself, but the pipeline's
# MAGIC export step cannot succeed without the UC secret -- so the seed resolves it first thing
# MAGIC and raises a clear, actionable error if it is unresolvable, failing at the first task
# MAGIC that could know instead of mid-pipeline-update. Provisioning that secret is a manual,
# MAGIC one-time, admin-audited step external to this bundle -- see the job resource header and
# MAGIC `metaflow_testing/README.md`'s "Sample reference suite" section.
# MAGIC
# MAGIC Idempotent: re-running an iteration overwrites the same landing files with the same
# MAGIC deterministic content.

# COMMAND ----------

import csv
import io
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_sample_04_export_encrypt_zip")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
dbutils.widgets.text("iteration", "1", "Which iteration to land (1, 2, or 3)")

CATALOG = dbutils.widgets.get("catalog").strip()
ITERATION = dbutils.widgets.get("iteration").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")
if ITERATION not in ("1", "2", "3"):
    raise ValueError(f"The 'iteration' widget must be '1', '2', or '3' -- got '{ITERATION}'.")

SAMPLE_SCHEMA = "metaflow_sample"
ZIP_PASSKEY_SECRET = "sample_zip_passkey"
LANDING_ROOT = f"/Volumes/{CATALOG}/{SAMPLE_SCHEMA}/landing"
ORDER_DATE = {"1": "2026-09-01", "2": "2026-09-02", "3": "2026-09-03"}[ITERATION]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fail-Fast Preflight: the UC Secret the Export Sinks Will Need

# COMMAND ----------

try:
    _passkey = dbutils.secrets.get(catalog=CATALOG, schema=SAMPLE_SCHEMA, key=ZIP_PASSKEY_SECRET)
    if not _passkey:
        raise ValueError("secret resolved to an empty value")
    logger.info(
        "Preflight OK: UC secret %s.%s.%s is resolvable (value redacted).", CATALOG, SAMPLE_SCHEMA, ZIP_PASSKEY_SECRET
    )
except Exception as exc:  # noqa: BLE001
    raise RuntimeError(
        f"Sample 04 requires the Unity Catalog secret {CATALOG}.{SAMPLE_SCHEMA}.{ZIP_PASSKEY_SECRET} "
        "-- the pipeline's pgp_zip sinks resolve it at export time to AES-256-password-protect every "
        "exported ZIP, so without it run_pipeline would fail mid-update. Provision it once "
        "per workspace (manual, admin-audited, external to this bundle; any non-empty string works -- "
        "pyzipper derives the AES key from the passphrase) and grant this job's principal READ SECRET "
        "on it, then re-run. See metaflow_testing/README.md's 'Sample reference suite' section for the "
        f"provisioning command. Original error: {exc}"
    ) from exc

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Provision the Single Sample Schema & Its Volumes

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SAMPLE_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SAMPLE_SCHEMA}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SAMPLE_SCHEMA}.exports")

logger.info("Provisioned %s.%s with the landing/exports volumes.", CATALOG, SAMPLE_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Helpers -- Deterministic samples-Catalog Slices with Inline Fallback

# COMMAND ----------


def _rows(query: str, fallback_data: list, fallback_schema: str, label: str) -> list:
    try:
        collected = spark.sql(query).collect()
        if not collected:
            raise ValueError("query returned zero rows")
        logger.info("Loaded %d %s row(s) from the Databricks samples catalog.", len(collected), label)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "samples catalog unavailable for %s (%s) -- falling back to an inline literal DataFrame (%d row(s)).",
            label,
            exc,
            len(fallback_data),
        )
        collected = spark.createDataFrame(fallback_data, fallback_schema).collect()
    return [row.asDict() for row in collected]


def _write_csv(path: str, fieldnames: list, rows: list) -> None:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    dbutils.fs.mkdirs(os.path.dirname(path))
    with open(path, "w", encoding="utf-8", newline="") as destination:
        destination.write(buffer.getvalue())
    logger.info("Landed %d row(s) at '%s'.", len(rows), path)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Orders (Streaming Driving Side) -- a Distinct 50-Row Slice per Iteration
# MAGIC
# MAGIC A handful of rows per slice deliberately exceed the 150000 `total_price` threshold so the
# MAGIC second (`high_value_orders`) export always has content.

# COMMAND ----------

_STATUSES = ["OPENED", "PROCESSING", "FULFILLED"]
_order_fallback = [
    (2000 + index, (index % 30) + 1, round(60000.0 + (index % 8) * 25000.0, 2))
    for index in range(150)
]
order_rows = _rows(
    """
    SELECT o_orderkey AS order_id, o_custkey AS customer_id, CAST(o_totalprice AS DOUBLE) AS total_price
    FROM samples.tpch.orders
    WHERE o_custkey <= 30
    ORDER BY o_orderkey
    LIMIT 150
    """,
    _order_fallback,
    "order_id LONG, customer_id LONG, total_price DOUBLE",
    "tpch orders",
)
_slice_start = (int(ITERATION) - 1) * 50
order_iteration_rows = [
    {
        "order_id": int(row["order_id"]),
        "customer_id": int(row["customer_id"]),
        "order_status": _STATUSES[index % 3],
        "total_price": round(float(row["total_price"]), 2),
        "order_date": ORDER_DATE,
    }
    for index, row in enumerate(order_rows[_slice_start : _slice_start + 50])
]

_write_csv(
    f"{LANDING_ROOT}/sample04_orders/incoming/orders_iter{ITERATION}.csv",
    ["order_id", "customer_id", "order_status", "total_price", "order_date"],
    order_iteration_rows,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Customers (Static Dimension Side) -- Base 1-30, Then a Few New Keys per Iteration

# COMMAND ----------

_SEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]
_customer_fallback = [
    (key, f"Customer#{key:09d}", _SEGMENTS[key % 5], key % 25)
    for key in range(1, 41)
]
customer_rows = _rows(
    """
    SELECT c_custkey AS customer_id, c_name AS customer_name, c_mktsegment AS market_segment,
           CAST(c_nationkey AS INT) AS nation_key
    FROM samples.tpch.customer
    WHERE c_custkey <= 40
    ORDER BY c_custkey
    """,
    _customer_fallback,
    "customer_id LONG, customer_name STRING, market_segment STRING, nation_key INT",
    "tpch customer",
)
_customer_key_range = {"1": (1, 30), "2": (31, 35), "3": (36, 40)}[ITERATION]
customer_iteration_rows = [
    {
        "customer_id": int(row["customer_id"]),
        "customer_name": row["customer_name"],
        "market_segment": row["market_segment"],
        "nation_key": int(row["nation_key"]),
    }
    for row in customer_rows
    if _customer_key_range[0] <= int(row["customer_id"]) <= _customer_key_range[1]
]

_write_csv(
    f"{LANDING_ROOT}/sample04_customers/incoming/customers_iter{ITERATION}.csv",
    ["customer_id", "customer_name", "market_segment", "nation_key"],
    customer_iteration_rows,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC Iteration landed. After this iteration's pipeline update, expect one new AES-256
# MAGIC password-protected archive per export sink per micro-batch under
# MAGIC `/Volumes/<catalog>/metaflow_sample/exports/{customer_orders,high_value_orders}/output/`
# MAGIC (openable with the `sample_zip_passkey` secret's value). Re-running this notebook with
# MAGIC the same `iteration` is safe: both landing files regenerate idempotently.
