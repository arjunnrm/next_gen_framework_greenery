# Databricks notebook source
# MAGIC %md
# MAGIC # UC3 Simulator -- Task B: `stream_producer`
# MAGIC
# MAGIC Stands in for ZeroBus/Kafka. Every `sleep_seconds` it reads the **next unconsumed
# MAGIC slice** of each source CSV under
# MAGIC `/Volumes/{catalog}/{schema}/uc_3/streaming/<table>/` and **appends** it into
# MAGIC `{catalog}.{schema}.<table>_stream`.
# MAGIC
# MAGIC ## Why this is hand-written code
# MAGIC
# MAGIC `BUILD_CONTRACT.md` 0.1: the simulator is the **one** legitimately custom artefact in
# MAGIC UC3, because there is no framework feature for "pretend to be a message bus".
# MAGIC Everything downstream -- CDC, hashing, tagging, SCD -- is FlowX configuration and is
# MAGIC deliberately **not** implemented here. This notebook only moves rows.
# MAGIC
# MAGIC ## The offset / chunking model
# MAGIC
# MAGIC Each source CSV is read **once per tick** and given a deterministic, stable row
# MAGIC number, then sliced:
# MAGIC
# MAGIC ```
# MAGIC row_num = row_number() OVER (ORDER BY <every source column>)   -- total, deterministic
# MAGIC slice   = rows where next_offset <= row_num - 1 < next_offset + chunk_size
# MAGIC ```
# MAGIC
# MAGIC The cursor lives in the Delta table `{catalog}.{schema}.uc3_simulator_offsets`
# MAGIC (created by Task A), keyed by `(table_name, source_signature)`. After each tick the
# MAGIC cursor advances by the number of rows actually emitted, so **every tick loads a
# MAGIC distinct slice -- never a full reload**, and the producer resumes correctly if the
# MAGIC job is re-run or killed mid-drain.
# MAGIC
# MAGIC `source_signature` fingerprints the source files (path, size, mtime). If the test data
# MAGIC is regenerated the signature changes and the cursor restarts from 0 -- which is what
# MAGIC you want, because the rows are new. Re-running against **unchanged** files resumes
# MAGIC where it left off and, once drained, does nothing.
# MAGIC
# MAGIC Ordering by every source column (rather than by an arbitrary column) makes the row
# MAGIC numbering reproducible across ticks even though the CSV is re-read each time. Without
# MAGIC a total order, `row_number()` could permute rows between ticks and the same row could
# MAGIC be emitted twice or skipped.
# MAGIC
# MAGIC ## Termination
# MAGIC
# MAGIC The loop **stops** when every table's cursor reaches `total_rows` -- it does not loop
# MAGIC forever. `max_ticks` is a hard belt-and-braces ceiling so a misconfiguration cannot
# MAGIC produce an unbounded job.
# MAGIC
# MAGIC ## Parallelism
# MAGIC
# MAGIC The three tables are drained **concurrently within one tick** by a
# MAGIC `ThreadPoolExecutor` of 3. Spark actions are thread-safe, and this keeps the three
# MAGIC tables advancing together so Job 2's streaming read sees all three progressing rather
# MAGIC than one table finishing before another starts.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC
# MAGIC Nothing is hardcoded in the body -- `flowx` / `staging` appear only as widget
# MAGIC defaults, per contract 1.

# COMMAND ----------

dbutils.widgets.text("catalog", "flowx", "Catalog")
dbutils.widgets.text("schema", "staging", "Staging schema")
dbutils.widgets.text("sleep_seconds", "20", "Seconds between ticks")
dbutils.widgets.text("chunk_size", "15", "Rows appended per table per tick")
dbutils.widgets.text("max_ticks", "200", "Safety ceiling on tick count")
dbutils.widgets.text("landing_subpath", "uc_3/streaming", "Path under /Volumes/<catalog>/<schema>/")
dbutils.widgets.text("reset_offsets", "false", "true = restart the drain from row 0")

# COMMAND ----------

import concurrent.futures
import logging
import os
import sys
import time
import traceback

from pyspark.sql import Window
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("uc3_stream_producer")


def _int_param(name, minimum, maximum):
    raw = dbutils.widgets.get(name).strip()
    try:
        value = int(raw)
    except ValueError:
        raise ValueError("`{n}` must be an integer, got {r!r}".format(n=name, r=raw))
    if not (minimum <= value <= maximum):
        raise ValueError("`{n}` must be between {lo} and {hi}, got {v}".format(n=name, lo=minimum, hi=maximum, v=value))
    return value


CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
SLEEP_SECONDS = _int_param("sleep_seconds", 0, 3600)
# Contract 4 asks for ~10-20 rows so 100 rows spread over multiple ticks, which is what
# gives Job 2's streaming read several micro-batches rather than one big one.
CHUNK_SIZE = _int_param("chunk_size", 1, 1000)
MAX_TICKS = _int_param("max_ticks", 1, 10000)
LANDING_SUBPATH = dbutils.widgets.get("landing_subpath").strip().strip("/")
RESET_OFFSETS = dbutils.widgets.get("reset_offsets").strip().lower() in ("true", "1", "yes")

if not CATALOG or not SCHEMA:
    raise ValueError("`catalog` and `schema` are required parameters and must be non-empty.")

OFFSET_TABLE = "`{c}`.`{s}`.`uc3_simulator_offsets`".format(c=CATALOG, s=SCHEMA)
LANDING_ROOT = "/Volumes/{c}/{s}/{p}".format(c=CATALOG, s=SCHEMA, p=LANDING_SUBPATH)

logger.info("catalog=%s schema=%s", CATALOG, SCHEMA)
logger.info("landing root  : %s", LANDING_ROOT)
logger.info("sleep_seconds=%s chunk_size=%s max_ticks=%s", SLEEP_SECONDS, CHUNK_SIZE, MAX_TICKS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared schema helper
# MAGIC
# MAGIC `uc3_ddl_schema.py` sits next to this notebook and owns the contract-6 parsing rules.
# MAGIC The producer needs it only for the table list and the `src_deleted_flg` convention --
# MAGIC the target schema itself is read back from the Delta table Task A created.

# COMMAND ----------


def _notebook_dir():
    if "__file__" in globals():
        return os.path.dirname(os.path.abspath(__file__))
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        return "/Workspace" + os.path.dirname(ctx.notebookPath().get())
    except Exception:  # noqa: BLE001
        return os.getcwd()


NOTEBOOK_DIR = _notebook_dir()
# The shared helper lives at src/uc3_simulator/, NOT alongside this notebook. Anything under
# notebooks/ is deployed by DABs as a NOTEBOOK, and Databricks refuses `import` on a notebook
# ("Unable to import module ... appears to be a notebook") -- which is exactly how the first
# Phase C run of this job failed. src/ is deployed as plain files, so it imports normally.
_REPO_ROOT = os.path.abspath(os.path.join(NOTEBOOK_DIR, "..", "..", ".."))
for _p in (os.path.join(_REPO_ROOT, "src", "uc3_simulator"), NOTEBOOK_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import uc3_ddl_schema  # noqa: E402

TABLES = list(uc3_ddl_schema.UC3_TABLES.keys())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source discovery and signature
# MAGIC
# MAGIC The signature is a fingerprint of the source file set. A regenerated test-data set
# MAGIC changes it, which restarts the drain; an unchanged set resumes.

# COMMAND ----------


def list_source_files(table):
    """CSV files landed for one table, in a stable order."""
    path = "{root}/{t}".format(root=LANDING_ROOT, t=table)
    try:
        entries = dbutils.fs.ls(path)
    except Exception as exc:  # noqa: BLE001
        raise FileNotFoundError(
            "No streaming landing directory for {t!r} at {p}. The test-data generator "
            "(scripts/generate_uc3_test_data.py) must run before the simulator. "
            "Original error: {e}".format(t=table, p=path, e=exc)
        ) from exc

    files = [e for e in entries if not e.isDir() and e.name.lower().endswith(".csv")]
    if not files:
        raise FileNotFoundError("No .csv files under {p} for table {t!r}.".format(p=path, t=table))
    return sorted(files, key=lambda e: e.path)


def source_signature(files):
    """Fingerprint of the source set: path, size and (when available) mtime."""
    parts = []
    for entry in files:
        mtime = getattr(entry, "modificationTime", None)
        parts.append("{p}:{s}:{m}".format(p=entry.path, s=entry.size, m=mtime if mtime is not None else "-"))
    return "|".join(parts)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading a deterministic, totally-ordered source frame
# MAGIC
# MAGIC The CSV is read with the **target table's schema**, so types line up with
# MAGIC `<table>_stream` exactly and no implicit-cast surprises reach the append. Columns the
# MAGIC generator does not supply (notably `src_deleted_flg`, and any `Drop(DF)` column that
# MAGIC must not exist) are reconciled here.

# COMMAND ----------


def target_columns(table):
    """(name, type) of the physical staging table, straight from the catalog."""
    df = spark.table("`{c}`.`{s}`.`{t}_stream`".format(c=CATALOG, s=SCHEMA, t=table))
    return [(f.name, f.dataType.simpleString()) for f in df.schema.fields]


def read_source(table):
    """Read one table's landed CSVs, projected onto the target schema.

    Returned frame has exactly the target columns, in target order. `src_deleted_flg` is
    defaulted to the 'present' marker when the generator did not emit it.
    """
    path = "{root}/{t}".format(root=LANDING_ROOT, t=table)
    target = target_columns(table)
    target_names = [name for name, _ in target]

    raw = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("recursiveFileLookup", "true")
        .csv(path)
    )
    available = set(raw.columns)

    projections = []
    for name, dtype in target:
        if name in available:
            projections.append(F.col("`{n}`".format(n=name)).cast(dtype).alias(name))
        elif name == uc3_ddl_schema.SRC_DELETED_FLG:
            # Contract 6.3: not a source column -- the simulator supplies it.
            projections.append(F.lit(uc3_ddl_schema.SRC_DELETED_FLG_PRESENT).cast(dtype).alias(name))
        else:
            projections.append(F.lit(None).cast(dtype).alias(name))

    # Any Drop(DF)=Y column that leaked into the CSV is discarded here by construction:
    # the projection is driven by the TARGET column list, not the source's.
    unexpected = sorted(available - set(target_names))
    if unexpected:
        logger.info("%-16s ignoring %d source column(s) absent from the target: %s", table, len(unexpected), unexpected)

    return raw.select(*projections), target_names


def numbered_source(table):
    """Source frame plus a deterministic, gapless `__row_num` starting at 1."""
    df, target_names = read_source(table)
    # A TOTAL order over every column: reproducible across ticks even though the CSV is
    # re-read each time. Ordering by a subset risks ties permuting between ticks, which
    # would double-emit or skip rows.
    order = [F.col("`{n}`".format(n=name)).asc_nulls_first() for name in target_names]
    window = Window.orderBy(*order)
    return df.withColumn("__row_num", F.row_number().over(window))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Cursor read / write
# MAGIC
# MAGIC One row per `(table_name, source_signature)` in the Delta cursor table.

# COMMAND ----------


def _sql_str(value):
    return "'" + str(value).replace("'", "''") + "'"


def read_cursor(table, signature, total_rows):
    """Current cursor for this (table, signature), initialising it if absent."""
    rows = spark.sql(
        "SELECT next_offset, rows_emitted, ticks, exhausted FROM {o} "
        "WHERE table_name = {t} AND source_signature = {s}".format(
            o=OFFSET_TABLE, t=_sql_str(table), s=_sql_str(signature)
        )
    ).collect()

    if rows and not RESET_OFFSETS:
        row = rows[0]
        return {
            "next_offset": int(row["next_offset"]),
            "rows_emitted": int(row["rows_emitted"]),
            "ticks": int(row["ticks"]),
            "exhausted": bool(row["exhausted"]),
        }

    # Either brand new, or an explicit reset. Clear any stale row for this key, and any
    # row for a superseded signature, so the cursor table stays one row per table.
    spark.sql("DELETE FROM {o} WHERE table_name = {t}".format(o=OFFSET_TABLE, t=_sql_str(table)))
    state = {"next_offset": 0, "rows_emitted": 0, "ticks": 0, "exhausted": total_rows == 0}
    write_cursor(table, signature, total_rows, state)
    return state


def write_cursor(table, signature, total_rows, state):
    spark.sql(
        """
        MERGE INTO {o} AS tgt
        USING (SELECT {t} AS table_name, {s} AS source_signature) AS src
          ON tgt.table_name = src.table_name AND tgt.source_signature = src.source_signature
        WHEN MATCHED THEN UPDATE SET
          tgt.next_offset  = {off},
          tgt.total_rows   = {tot},
          tgt.rows_emitted = {emit},
          tgt.ticks        = {ticks},
          tgt.exhausted    = {done},
          tgt.updated_at   = current_timestamp()
        WHEN NOT MATCHED THEN INSERT
          (table_name, source_signature, next_offset, total_rows, rows_emitted, ticks, exhausted, updated_at)
          VALUES ({t}, {s}, {off}, {tot}, {emit}, {ticks}, {done}, current_timestamp())
        """.format(
            o=OFFSET_TABLE,
            t=_sql_str(table),
            s=_sql_str(signature),
            off=int(state["next_offset"]),
            tot=int(total_rows),
            emit=int(state["rows_emitted"]),
            ticks=int(state["ticks"]),
            done="true" if state["exhausted"] else "false",
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## One tick for one table
# MAGIC
# MAGIC Read -> slice -> append -> advance the cursor. The cursor advances by the number of
# MAGIC rows **actually written**, so a partial slice at the tail of the file is handled
# MAGIC correctly and nothing is skipped.

# COMMAND ----------


def drain_one_tick(table):
    """Append the next slice for one table. Returns a per-table status dict."""
    files = list_source_files(table)
    signature = source_signature(files)

    # NO .cache()/.persist(): serverless compute rejects it outright
    # ("[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless
    # compute"), which is how the second Phase C run of this job failed. The cache was
    # only an optimisation -- `numbered` is a deterministic projection of the same
    # immutable CSVs (row_number over a total ordering), so recomputing it per action
    # yields identical rows; only the recompute cost differs, and at 100 rows/table
    # that is immaterial.
    numbered = numbered_source(table)
    try:
        total_rows = numbered.count()
        state = read_cursor(table, signature, total_rows)

        if state["exhausted"] or state["next_offset"] >= total_rows:
            state["exhausted"] = True
            write_cursor(table, signature, total_rows, state)
            return {"table": table, "emitted": 0, "next_offset": state["next_offset"], "total": total_rows, "done": True}

        start = state["next_offset"]
        end = min(start + CHUNK_SIZE, total_rows)

        slice_df = numbered.filter(
            (F.col("__row_num") > F.lit(start)) & (F.col("__row_num") <= F.lit(end))
        ).drop("__row_num")

        target_name = "{c}.{s}.{t}_stream".format(c=CATALOG, s=SCHEMA, t=table)
        # `slice_df` was projected onto the target schema in `read_source`, so its columns
        # already match the table by name, order and type. mergeSchema stays off so an
        # unexpected column is a hard failure rather than a silent schema drift.
        slice_df.write.format("delta").mode("append").option("mergeSchema", "false").saveAsTable(target_name)

        emitted = end - start
        state["next_offset"] = end
        state["rows_emitted"] += emitted
        state["ticks"] += 1
        state["exhausted"] = end >= total_rows
        write_cursor(table, signature, total_rows, state)

        logger.info(
            "%-16s +%3d rows -> %s  [%d/%d]%s",
            table,
            emitted,
            target_name,
            end,
            total_rows,
            "  DONE" if state["exhausted"] else "",
        )
        return {
            "table": table,
            "emitted": emitted,
            "next_offset": end,
            "total": total_rows,
            "done": state["exhausted"],
        }
    finally:
        pass  # nothing to unpersist -- see the no-cache note above


# COMMAND ----------

# MAGIC %md
# MAGIC ## The drain loop
# MAGIC
# MAGIC Ticks until every table is exhausted, or `max_ticks` is hit. The three tables are
# MAGIC drained **in parallel within each tick**.

# COMMAND ----------

if RESET_OFFSETS:
    logger.warning("reset_offsets=true -- the drain restarts from row 0 for all three tables.")

pending = set(TABLES)
totals = {t: {"emitted": 0, "total": 0} for t in TABLES}
tick = 0
started = time.time()
failures = []

while pending and tick < MAX_TICKS:
    tick += 1
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(TABLES)) as pool:
        futures = {pool.submit(drain_one_tick, t): t for t in sorted(pending)}
        results = {}
        for future in concurrent.futures.as_completed(futures):
            table = futures[future]
            try:
                results[table] = future.result()
            except Exception as exc:  # noqa: BLE001
                failures.append((table, exc, traceback.format_exc()))
                logger.error("%-16s tick %d FAILED: %s", table, tick, exc)
                # Stop scheduling this table; the run fails at the end with the detail.
                pending.discard(table)

    if failures:
        break

    for table, result in results.items():
        totals[table]["emitted"] += result["emitted"]
        totals[table]["total"] = result["total"]
        if result["done"]:
            pending.discard(table)

    logger.info(
        "tick %d complete | remaining tables: %s",
        tick,
        sorted(pending) if pending else "(none -- drain finished)",
    )

    # Only sleep if there is more work to do -- no dead time at the end of the drain.
    if pending and SLEEP_SECONDS > 0:
        time.sleep(SLEEP_SECONDS)

elapsed = time.time() - started

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completion summary

# COMMAND ----------

lines = [
    "UC3 stream_producer finished.",
    "",
    "  ticks executed : {t}".format(t=tick),
    "  elapsed        : {e:.1f}s (sleep_seconds={s}, chunk_size={c})".format(e=elapsed, s=SLEEP_SECONDS, c=CHUNK_SIZE),
    "  landing root   : {r}".format(r=LANDING_ROOT),
    "",
]
for table in TABLES:
    stats = totals[table]
    lines.append(
        "  {t:<16} appended {e:>4} row(s) this run | source total {n}".format(
            t=table, e=stats["emitted"], n=stats["total"]
        )
    )
lines.append("")

if failures:
    lines.append("  STATUS: FAILED -- {n} table(s) errored.".format(n=len(failures)))
    for table, exc, tb in failures:
        lines.append("    {t}: {e}".format(t=table, e=exc))
    summary = "\n".join(lines)
    print(summary)
    for table, exc, tb in failures:
        logger.error("traceback for %s:\n%s", table, tb)
    raise RuntimeError(summary)

if pending:
    lines.append(
        "  STATUS: STOPPED at the max_ticks ceiling ({m}) with {p} table(s) not yet "
        "drained: {t}. Raise max_ticks or chunk_size, or re-run -- the cursor resumes.".format(
            m=MAX_TICKS, p=len(pending), t=sorted(pending)
        )
    )
else:
    lines.append("  STATUS: all three tables fully drained. Re-running is a no-op until the test data changes.")

summary = "\n".join(lines)
print(summary)
logger.info("stream_producer complete")

dbutils.notebook.exit(summary)
