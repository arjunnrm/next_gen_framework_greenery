# Databricks notebook source
# MAGIC %md
# MAGIC # Seed Sample 01 -- Multi-Strategy SCD Dimension Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `resources/sample_jobs/onboarding/sample_01_multi_scd.json` only.
# MAGIC Parameterized by `iteration`; the common seed job
# MAGIC (`resources/sample_jobs/metaflow_sample_seed_job.yml`) invokes it once per iteration
# MAGIC (1 -> 2 -> 3, chained) before any sample pipeline runs. Data comes from the
# MAGIC Databricks `samples` catalog (`samples.tpch.customer/supplier/part/orders`) as
# MAGIC deterministic key-range slices, falling back to small inline literal DataFrames when
# MAGIC `samples` is not shared into this workspace (availability differs per workspace -- the
# MAGIC log says which path was taken).
# MAGIC
# MAGIC | Feed (landing dir under `landing/`) | Strategy | iteration 1 | iteration 2 | iteration 3 |
# MAGIC |---|---|---|---|---|
# MAGIC | `sample01_customers` | SCD1 | keys 1-50, base values | changed segment/balance for keys % 5 == 0 (overwrite in place) | NEW keys 51-60 + balance drop for keys % 7 == 0 |
# MAGIC | `sample01_suppliers` | SCD2 | keys 1-30, base values | balance +100 for keys % 3 == 0 (opens new history versions) | NEW keys 31-40 + nation move for keys % 4 == 0 |
# MAGIC | `sample01_parts` | FULL_SNAPSHOT_CDC | keys 1-40 | keys 41-60 (NEW keys only) | keys 61-80 (NEW keys only) |
# MAGIC | `sample01_orders` | APPEND (feeds SCD3) | 20 orders, status OPENED | same 20 orders, PROCESSING | same 20 orders, FULFILLED |
# MAGIC
# MAGIC **The parts feed is strictly ADDITIVE by design** (never a changed or removed existing
# MAGIC key): `apply_changes_from_snapshot`'s snapshot-input dataset accumulates over a streaming
# MAGIC Auto Loader upstream, so a re-landed existing key would appear twice in one accumulated
# MAGIC snapshot and a removed key would never be deleted from the target -- see the spec's
# MAGIC `_sample_note` and `cdc/snapshot.py`'s KNOWN LIMITATION comment.
# MAGIC
# MAGIC Idempotent: re-running an iteration overwrites the same landing file with the same
# MAGIC deterministic content, and all schema/volume provisioning is `CREATE ... IF NOT EXISTS`.

# COMMAND ----------

import csv
import io
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_sample_01_multi_scd")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
dbutils.widgets.text("iteration", "1", "Which iteration to land (1, 2, or 3)")

CATALOG = dbutils.widgets.get("catalog").strip()
ITERATION = dbutils.widgets.get("iteration").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")
if ITERATION not in ("1", "2", "3"):
    raise ValueError(f"The 'iteration' widget must be '1', '2', or '3' -- got '{ITERATION}'.")

SAMPLE_SCHEMA = "metaflow_sample"
LANDING_ROOT = f"/Volumes/{CATALOG}/{SAMPLE_SCHEMA}/landing"
EVENT_TS = {"1": "2026-09-01 00:00:00", "2": "2026-09-02 00:00:00", "3": "2026-09-03 00:00:00"}[ITERATION]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision the Single Sample Schema & Its Volumes
# MAGIC
# MAGIC Everything Sample 01 touches lives in `metaflow_sample`: landing files, `_schemas`
# MAGIC checkpoint dirs (inside the `landing` Volume), targets, and the observability export
# MAGIC Volume the spec's `DATABRICKS_VOLUME` destination writes under.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SAMPLE_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SAMPLE_SCHEMA}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SAMPLE_SCHEMA}.observability")

logger.info("Provisioned %s.%s with the landing/observability volumes.", CATALOG, SAMPLE_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Helpers -- Deterministic samples-Catalog Slices with Inline Fallback

# COMMAND ----------


def _rows(query: str, fallback_data: list, fallback_schema: str, label: str) -> list:
    """Deterministic slice from the Databricks `samples` catalog; falls back to a small inline
    literal DataFrame when that catalog is not shared into this workspace. Logs which path was
    taken so a run's provenance is always visible."""
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
    """Write one deterministic CSV landing file directly into the Volume (single file, never a
    Spark part-file directory), overwriting idempotently on re-run."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    dbutils.fs.mkdirs(os.path.dirname(path))
    with open(path, "w", encoding="utf-8", newline="") as destination:
        destination.write(buffer.getvalue())
    logger.info("Landed %d row(s) at '%s'.", len(rows), path)


_SEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SCD1 Customers (`sample01_customers`)

# COMMAND ----------

_customer_fallback = [
    (key, f"Customer#{key:09d}", _SEGMENTS[key % 5], round(1000.0 + key * 13.7, 2), key % 25)
    for key in range(1, 61)
]
customer_rows = _rows(
    """
    SELECT c_custkey AS customer_id, c_name AS customer_name, c_mktsegment AS market_segment,
           CAST(c_acctbal AS DOUBLE) AS account_balance, CAST(c_nationkey AS INT) AS nation_key
    FROM samples.tpch.customer
    WHERE c_custkey <= 60
    ORDER BY c_custkey
    """,
    _customer_fallback,
    "customer_id LONG, customer_name STRING, market_segment STRING, account_balance DOUBLE, nation_key INT",
    "tpch customer",
)
customers_by_key = {int(row["customer_id"]): row for row in customer_rows}


def _customer_row(key: int, segment=None, balance_delta: float = 0.0) -> dict:
    base = customers_by_key[key]
    return {
        "customer_id": key,
        "customer_name": base["customer_name"],
        "market_segment": segment or base["market_segment"],
        "account_balance": round(float(base["account_balance"]) + balance_delta, 2),
        "nation_key": base["nation_key"],
        "updated_at": EVENT_TS,
    }


existing_customer_keys = [key for key in sorted(customers_by_key) if key <= 50]
if ITERATION == "1":
    customer_iteration_rows = [_customer_row(key) for key in existing_customer_keys]
elif ITERATION == "2":
    # Changed customers only -- SCD1 overwrites these keys in place, no history kept.
    customer_iteration_rows = [
        _customer_row(key, segment="PLATINUM", balance_delta=250.0) for key in existing_customer_keys if key % 5 == 0
    ]
else:
    # Brand-new keys 51-60 (inserted) plus a further balance change for keys % 7 == 0.
    new_customer_keys = [key for key in sorted(customers_by_key) if 51 <= key <= 60]
    customer_iteration_rows = [_customer_row(key) for key in new_customer_keys] + [
        _customer_row(key, balance_delta=-50.0) for key in existing_customer_keys if key % 7 == 0
    ]

_write_csv(
    f"{LANDING_ROOT}/sample01_customers/incoming/customers_iter{ITERATION}.csv",
    ["customer_id", "customer_name", "market_segment", "account_balance", "nation_key", "updated_at"],
    customer_iteration_rows,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. SCD2 Suppliers (`sample01_suppliers`)

# COMMAND ----------

_supplier_fallback = [
    (key, f"Supplier#{key:09d}", key % 25, round(2000.0 + key * 21.3, 2))
    for key in range(1, 41)
]
supplier_rows = _rows(
    """
    SELECT s_suppkey AS supplier_id, s_name AS supplier_name, CAST(s_nationkey AS INT) AS nation_key,
           CAST(s_acctbal AS DOUBLE) AS account_balance
    FROM samples.tpch.supplier
    WHERE s_suppkey <= 40
    ORDER BY s_suppkey
    """,
    _supplier_fallback,
    "supplier_id LONG, supplier_name STRING, nation_key INT, account_balance DOUBLE",
    "tpch supplier",
)
suppliers_by_key = {int(row["supplier_id"]): row for row in supplier_rows}


def _supplier_row(key: int, balance_delta: float = 0.0, move_nation: bool = False) -> dict:
    base = suppliers_by_key[key]
    nation_key = int(base["nation_key"])
    return {
        "supplier_id": key,
        "supplier_name": base["supplier_name"],
        "nation_key": (nation_key + 1) % 25 if move_nation else nation_key,
        "account_balance": round(float(base["account_balance"]) + balance_delta, 2),
        "updated_at": EVENT_TS,
    }


existing_supplier_keys = [key for key in sorted(suppliers_by_key) if key <= 30]
if ITERATION == "1":
    supplier_iteration_rows = [_supplier_row(key) for key in existing_supplier_keys]
elif ITERATION == "2":
    # account_balance is in columns_to_check -- each of these opens a new SCD2 history version.
    supplier_iteration_rows = [_supplier_row(key, balance_delta=100.0) for key in existing_supplier_keys if key % 3 == 0]
else:
    new_supplier_keys = [key for key in sorted(suppliers_by_key) if 31 <= key <= 40]
    supplier_iteration_rows = [_supplier_row(key) for key in new_supplier_keys] + [
        _supplier_row(key, move_nation=True) for key in existing_supplier_keys if key % 4 == 0
    ]

_write_csv(
    f"{LANDING_ROOT}/sample01_suppliers/incoming/suppliers_iter{ITERATION}.csv",
    ["supplier_id", "supplier_name", "nation_key", "account_balance", "updated_at"],
    supplier_iteration_rows,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. FULL_SNAPSHOT_CDC Parts (`sample01_parts`) -- STRICTLY ADDITIVE
# MAGIC
# MAGIC New part keys only, per iteration: 1-40, then 41-60, then 61-80. Never a changed or
# MAGIC removed existing key -- the accumulated snapshot input would otherwise carry duplicate
# MAGIC keys / never delete (see the notebook header).

# COMMAND ----------

_part_fallback = [
    (key, f"Part#{key:06d}", f"Brand#{(key % 5) + 1}{(key % 3) + 1}", "STANDARD ANODIZED", round(900.0 + key * 1.01, 2))
    for key in range(1, 81)
]
part_rows = _rows(
    """
    SELECT p_partkey AS part_id, p_name AS part_name, p_brand AS brand, p_type AS part_type,
           CAST(p_retailprice AS DOUBLE) AS retail_price
    FROM samples.tpch.part
    WHERE p_partkey <= 80
    ORDER BY p_partkey
    """,
    _part_fallback,
    "part_id LONG, part_name STRING, brand STRING, part_type STRING, retail_price DOUBLE",
    "tpch part",
)
_part_key_range = {"1": (1, 40), "2": (41, 60), "3": (61, 80)}[ITERATION]
part_iteration_rows = [
    {
        "part_id": int(row["part_id"]),
        "part_name": row["part_name"],
        "brand": row["brand"],
        "part_type": row["part_type"],
        "retail_price": round(float(row["retail_price"]), 2),
    }
    for row in part_rows
    if _part_key_range[0] <= int(row["part_id"]) <= _part_key_range[1]
]

_write_csv(
    f"{LANDING_ROOT}/sample01_parts/incoming/parts_iter{ITERATION}.csv",
    ["part_id", "part_name", "brand", "part_type", "retail_price"],
    part_iteration_rows,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. APPEND Order Events (`sample01_orders`) -- the SCD3 Upstream
# MAGIC
# MAGIC The SAME 20 orders advance one status per iteration (OPENED -> PROCESSING -> FULFILLED),
# MAGIC so after the third pipeline update the SCD3 target shows
# MAGIC `current_order_status = FULFILLED` / `previous_order_status = PROCESSING` for every order.

# COMMAND ----------

_order_fallback = [
    (100 + index, (index % 50) + 1, round(25000.0 + index * 4321.5, 2))
    for index in range(20)
]
order_rows = _rows(
    """
    SELECT o_orderkey AS order_id, o_custkey AS customer_id, CAST(o_totalprice AS DOUBLE) AS total_price
    FROM samples.tpch.orders
    WHERE o_custkey <= 50
    ORDER BY o_orderkey
    LIMIT 20
    """,
    _order_fallback,
    "order_id LONG, customer_id LONG, total_price DOUBLE",
    "tpch orders",
)
_status = {"1": "OPENED", "2": "PROCESSING", "3": "FULFILLED"}[ITERATION]
order_iteration_rows = [
    {
        "order_id": int(row["order_id"]),
        "customer_id": int(row["customer_id"]),
        "order_status": _status,
        "total_price": round(float(row["total_price"]), 2),
        "event_ts": EVENT_TS,
    }
    for row in order_rows
]

_write_csv(
    f"{LANDING_ROOT}/sample01_orders/incoming/order_events_iter{ITERATION}.csv",
    ["order_id", "customer_id", "order_status", "total_price", "event_ts"],
    order_iteration_rows,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC Iteration landed. `resources/sample_jobs/onboarding/sample_01_multi_scd.json` can now be (or
# MAGIC already was) onboarded and the sample pipeline run for this iteration. Re-running this
# MAGIC notebook with the same `iteration` is safe: every landing file regenerates idempotently
# MAGIC (same deterministic content, overwritten in place).
