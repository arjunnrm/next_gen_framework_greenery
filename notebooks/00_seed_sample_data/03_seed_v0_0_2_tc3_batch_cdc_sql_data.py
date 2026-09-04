# Databricks notebook source
# MAGIC %md
# MAGIC # Seed v0.0.2 TEST CASE 3 -- Batch + CDC + SQL Transformation + JSON, METRICS/LOGS OFF
# MAGIC
# MAGIC Dedicated seed notebook for `flowx_testing/v0_0_2_tc3_batch_cdc_sql_metrics_off.json`
# MAGIC only -- deliberately separate from `02_seed_flowx_testing_data.py` and from every other
# MAGIC scenario's own seed notebook, so this test case's build/run stays isolated.
# MAGIC
# MAGIC ## What this provisions
# MAGIC
# MAGIC The spec's two batch ingestion flows read from two Auto Loader landing zones that no other
# MAGIC fixture in this repo creates:
# MAGIC
# MAGIC | Landing zone | Feeds Bronze table | Notes |
# MAGIC |---|---|---|
# MAGIC | `/Volumes/<catalog>/tc3_usecase/landing_orders/incoming/` | `bronze_tc3.tc3_orders_bronze` | order CDC feed; `order_payload` is a **JSON string** column |
# MAGIC | `/Volumes/<catalog>/tc3_usecase/landing_order_status/incoming/` | `bronze_tc3.tc3_order_status_bronze` | order-status reference feed |
# MAGIC
# MAGIC Unlike the older per-scenario seed notebooks, this one does **not** read CSV fixtures from
# MAGIC `sample_data/`: the rows are small, and generating them here keeps the whole test case
# MAGIC self-contained in the three files the scenario owns plus this notebook.
# MAGIC
# MAGIC ## What the fixture data is designed to prove
# MAGIC
# MAGIC * **CDC latest-state resolution (SCD1).** `ORD-1001` appears twice (`I` then `U`) so the
# MAGIC   SCD1 transformation must keep only the later `updated_at` row. `ORD-1004` appears as an
# MAGIC   insert and then with `cdc_op = D`, so `apply_as_deletes` must physically remove it from
# MAGIC   `silver_tc3.tc3_order_current`. `ORD-1002` / `ORD-1003` are plain single inserts.
# MAGIC * **JSON conversion.** Every `order_payload` value is a JSON document held in a STRING
# MAGIC   column, matching the spec's `json_string_columns` `schema_ddl`
# MAGIC   `struct<channel:string,coupon_code:string,line_item_count:int,ship_country:string>`.
# MAGIC   `ORD-1003` deliberately carries a NULL `ship_country` inside the JSON so the SQL
# MAGIC   transformation's `WHERE ... ship_country IS NOT NULL` filter drops a real row rather
# MAGIC   than filtering nothing.
# MAGIC * **SQL transformation derivations.** `discount_rate` on the status feed drives
# MAGIC   `net_order_amount`; the `coupon_code` presence/absence drives `is_discounted`; the order
# MAGIC   amounts straddle the 100 / 1000 `order_amount_band` boundaries.
# MAGIC * **A zero-amount row** (`ORD-1005`) so the `order_amount > 0` filter also drops a row.
# MAGIC
# MAGIC ## Telemetry stays OFF
# MAGIC
# MAGIC This notebook seeds source data ONLY. It never writes to `reconciliation_run_log`, to any
# MAGIC mismatch log, or to any `recon__*__metrics` dataset -- the whole point of TC3 is that a
# MAGIC full pipeline update produces **zero** such rows, so a seed step that pre-created any of
# MAGIC them would destroy the assertion the test case exists to make.
# MAGIC
# MAGIC Run once per environment before onboarding
# MAGIC `flowx_testing/v0_0_2_tc3_batch_cdc_sql_metrics_off.json`.

# COMMAND ----------

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_v0_0_2_tc3_batch_cdc_sql_data")

dbutils.widgets.text("catalog", "flowx", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

logger.info("Seeding v0.0.2 TC3 fixtures into catalog '%s'.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision the `tc3_usecase` schema and its landing / schema Volumes
# MAGIC
# MAGIC `bronze_tc3` and `silver_tc3` (the Bronze and Silver target schemas) are created by the
# MAGIC pipeline's own `CREATE SCHEMA IF NOT EXISTS` at deployment time, same as every other
# MAGIC scenario -- they are deliberately not pre-created here.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.tc3_usecase")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.tc3_usecase.landing_orders")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.tc3_usecase.landing_order_status")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.tc3_usecase._schemas")

ORDERS_INCOMING = f"/Volumes/{CATALOG}/tc3_usecase/landing_orders/incoming"
STATUS_INCOMING = f"/Volumes/{CATALOG}/tc3_usecase/landing_order_status/incoming"

dbutils.fs.mkdirs(ORDERS_INCOMING)
dbutils.fs.mkdirs(STATUS_INCOMING)

logger.info("Provisioned tc3_usecase schema + landing_orders/landing_order_status/_schemas volumes.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the order CDC feed
# MAGIC
# MAGIC `order_payload` is quoted CSV containing a JSON document, so the embedded `"` characters
# MAGIC are CSV-escaped by doubling them (`""`) -- the standard RFC-4180 escape Spark's CSV reader
# MAGIC expects. The spec reads this file with `header: true` and
# MAGIC `cloudFiles.inferColumnTypes: true`, so `order_payload` still arrives as a STRING (a JSON
# MAGIC document is not a CSV-inferable type) and is converted to a native struct by
# MAGIC `json_string_columns`.

# COMMAND ----------

ORDERS_CSV = '\n'.join([
    "order_id,customer_id,order_status,order_amount,currency_code,order_payload,updated_at,cdc_op",
    # ORD-1001 insert, then update -- SCD1 must keep only the 09:30 row (amount 1250.00, PLATINUM band HIGH).
    'ORD-1001,CUST-01,NEW,900.00,USD,"{""channel"":""WEB"",""coupon_code"":""SAVE10"",""line_item_count"":3,""ship_country"":""US""}",2026-09-01T09:00:00.000+00:00,I',
    'ORD-1001,CUST-01,CONFIRMED,1250.00,USD,"{""channel"":""WEB"",""coupon_code"":""SAVE10"",""line_item_count"":5,""ship_country"":""US""}",2026-09-01T09:30:00.000+00:00,U',
    # ORD-1002 plain insert, no coupon -> is_discounted false, band MEDIUM.
    'ORD-1002,CUST-02,CONFIRMED,450.50,EUR,"{""channel"":""MOBILE"",""coupon_code"":null,""line_item_count"":2,""ship_country"":""DE""}",2026-09-01T09:05:00.000+00:00,I',
    # ORD-1003 has a NULL ship_country inside the JSON -> dropped by the transformation's WHERE filter.
    'ORD-1003,CUST-03,CONFIRMED,75.00,GBP,"{""channel"":""STORE"",""coupon_code"":null,""line_item_count"":1,""ship_country"":null}",2026-09-01T09:10:00.000+00:00,I',
    # ORD-1004 inserted then DELETED -> apply_as_deletes must physically remove it from tc3_order_current.
    'ORD-1004,CUST-04,NEW,320.00,USD,"{""channel"":""WEB"",""coupon_code"":""WELCOME"",""line_item_count"":2,""ship_country"":""CA""}",2026-09-01T09:15:00.000+00:00,I',
    'ORD-1004,CUST-04,CANCELLED,320.00,USD,"{""channel"":""WEB"",""coupon_code"":""WELCOME"",""line_item_count"":2,""ship_country"":""CA""}",2026-09-01T09:45:00.000+00:00,D',
    # ORD-1005 has a zero amount -> survives SCD1 but is dropped by the transformation's order_amount > 0 filter.
    'ORD-1005,CUST-05,CONFIRMED,0.00,USD,"{""channel"":""WEB"",""coupon_code"":null,""line_item_count"":1,""ship_country"":""US""}",2026-09-01T09:20:00.000+00:00,I',
    "",
])

dbutils.fs.put(f"{ORDERS_INCOMING}/tc3_orders_cdc_day1.csv", ORDERS_CSV, overwrite=True)
logger.info("Landed order CDC feed at '%s/tc3_orders_cdc_day1.csv' (7 rows, 5 distinct order_ids).", ORDERS_INCOMING)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Land the order-status reference feed
# MAGIC
# MAGIC This is the second physical source table the test case requires. It is joined by the SQL
# MAGIC transformation on `order_status` (supplying `status_description` and the `discount_rate`
# MAGIC that drives `net_order_amount`), and it is also the reconciliation flow's comparison
# MAGIC target side -- the flow compares `order_status` / `order_status_description` between
# MAGIC `silver_tc3.tc3_order_enriched` and this table, `target_to_source`, with no
# MAGIC `append_target_table` and therefore no healing write back into it.
# MAGIC
# MAGIC `RETURNED` is present here but matched by no order, which gives the `target_to_source`
# MAGIC comparison a genuine unmatched row to classify -- and, because both capture flags are
# MAGIC false, that classification must still produce **zero** logged rows.

# COMMAND ----------

STATUS_CSV = '\n'.join([
    "order_status,status_description,discount_rate,is_terminal",
    "NEW,Order received and awaiting confirmation,0.00,false",
    "CONFIRMED,Order confirmed and queued for fulfilment,0.05,false",
    "SHIPPED,Order dispatched to the carrier,0.00,false",
    "CANCELLED,Order cancelled before fulfilment,0.00,true",
    "RETURNED,Order returned by the customer,0.00,true",
    "",
])

dbutils.fs.put(f"{STATUS_INCOMING}/tc3_order_status_day1.csv", STATUS_CSV, overwrite=True)
logger.info("Landed order-status reference feed at '%s/tc3_order_status_day1.csv' (5 rows).", STATUS_INCOMING)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done -- expected end state after one pipeline update
# MAGIC
# MAGIC | Dataset | Expected |
# MAGIC |---|---|
# MAGIC | `bronze_tc3.tc3_orders_bronze` | 7 rows, `order_payload` a native `struct<...>` (not a string) |
# MAGIC | `bronze_tc3.tc3_order_status_bronze` | 5 rows |
# MAGIC | `silver_tc3.tc3_order_current` | 4 rows -- `ORD-1004` deleted by `apply_as_deletes`; `ORD-1001` present once at `order_amount = 1250.00` |
# MAGIC | `silver_tc3.tc3_order_enriched` | 2 rows -- `ORD-1003` dropped (NULL `ship_country`), `ORD-1005` dropped (zero amount) |
# MAGIC | `reconciliation_run_log` | **0 new rows** (`run_log_capture: false`) |
# MAGIC | mismatch log | **0 new rows** (`mismatch_log_capture: false`) |
# MAGIC | `recon__recon_tc3_enriched_vs_status_metrics_off__*__metrics` | **not registered at all** |
# MAGIC
# MAGIC The last three rows are the assertion this test case exists to make -- verify them before
# MAGIC calling the run a pass.
# MAGIC
# MAGIC Re-running this notebook is safe and idempotent: it re-writes the same two filenames, and
# MAGIC Auto Loader tracks already-ingested files through its own checkpoint, so re-landing an
# MAGIC identical filename does not re-ingest or duplicate rows.
