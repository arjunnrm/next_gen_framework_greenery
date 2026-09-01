# Databricks notebook source
# MAGIC %md
# MAGIC # Seed Sample 05 -- Encrypted ZIP Ingestion Fixtures (AES-256 Passkey)
# MAGIC
# MAGIC Dedicated seed notebook for
# MAGIC `metaflow_testing/samples/sample_05_encrypted_ingestion.json` only. Parameterized by
# MAGIC `iteration`; the common seed job
# MAGIC (`resources/sample_jobs/metaflow_sample_seed_job.yml`) invokes it once per iteration
# MAGIC (1 -> 2 -> 3, chained) before any sample pipeline runs.
# MAGIC
# MAGIC Each iteration builds ONE AES-256 password-protected ZIP **in-process with `pyzipper`**
# MAGIC (the same library `archive/zip_utils.py::extract_encrypted_zip` uses to open it at
# MAGIC pipeline runtime -- and never a pre-built binary fixture, workspace-bundle sync can mangle
# MAGIC those) from a DISTINCT deterministic 40-row transaction slice derived from
# MAGIC `samples.tpch.orders` (rows 0-39 / 40-79 / 80-119 of a 120-row window), protected with
# MAGIC the UC secret `<catalog>.metaflow_sample.sample_zip_passkey` -- the SAME secret the
# MAGIC spec's `source_zip_handling.pre_extraction_decryption.secret_passphrase` resolves to
# MAGIC decrypt it on ingestion, and the same one Sample 04 uses on its export side. Each built
# MAGIC archive is round-trip-verified (re-opened with the same passphrase) before landing.
# MAGIC
# MAGIC **Fail-fast:** the passphrase is REQUIRED to build the archives, so this seed resolves
# MAGIC the secret first thing and raises a clear, actionable error if it is unresolvable.
# MAGIC Provisioning it is a manual, one-time, admin-audited step external to this bundle -- see
# MAGIC the job resource header and `metaflow_testing/README.md`'s "Sample reference suite"
# MAGIC section. Falls back to a small inline literal DataFrame when the `samples` catalog is not
# MAGIC shared into this workspace (the log says which path was taken). Idempotent per iteration.

# COMMAND ----------

import csv
import io
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_sample_05_encrypted_ingestion")

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
TXN_TS = {"1": "2026-09-01 00:00:00", "2": "2026-09-02 00:00:00", "3": "2026-09-03 00:00:00"}[ITERATION]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fail-Fast: Resolve the UC Secret This Seed Itself Needs

# COMMAND ----------

try:
    PASSKEY = dbutils.secrets.get(catalog=CATALOG, schema=SAMPLE_SCHEMA, key=ZIP_PASSKEY_SECRET)
    if not PASSKEY:
        raise ValueError("secret resolved to an empty value")
    logger.info(
        "Resolved UC secret %s.%s.%s for archive encryption (value redacted).",
        CATALOG,
        SAMPLE_SCHEMA,
        ZIP_PASSKEY_SECRET,
    )
except Exception as exc:  # noqa: BLE001
    raise RuntimeError(
        f"Sample 05 requires the Unity Catalog secret {CATALOG}.{SAMPLE_SCHEMA}.{ZIP_PASSKEY_SECRET} "
        "-- this seed encrypts every landed archive with it, and the pipeline's "
        "source_zip_handling.pre_extraction_decryption.secret_passphrase resolves the same secret to "
        "decrypt them. Provision it once per workspace (manual, admin-audited, external to this "
        "bundle; any non-empty string works -- pyzipper derives the AES key from the passphrase) and "
        "grant this job's principal READ SECRET on it, then re-run. See metaflow_testing/README.md's "
        f"'Sample reference suite' section for the provisioning command. Original error: {exc}"
    ) from exc

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Provision the Single Sample Schema & Landing Volume

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SAMPLE_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SAMPLE_SCHEMA}.landing")

logger.info("Provisioned %s.%s with the landing volume.", CATALOG, SAMPLE_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build This Iteration's Transaction Slice (samples Catalog, Inline Fallback)

# COMMAND ----------

_txn_fallback = [
    (3000 + index, (index % 60) + 1, round(120.0 + index * 37.11, 2))
    for index in range(120)
]

try:
    _collected = spark.sql(
        """
        SELECT o_orderkey AS txn_id, o_custkey AS account_id, CAST(o_totalprice AS DOUBLE) AS amount
        FROM samples.tpch.orders
        WHERE o_custkey BETWEEN 51 AND 150
        ORDER BY o_orderkey
        LIMIT 120
        """
    ).collect()
    if not _collected:
        raise ValueError("query returned zero rows")
    base_rows = [row.asDict() for row in _collected]
    logger.info("Loaded %d transaction row(s) from the Databricks samples catalog.", len(base_rows))
except Exception as exc:  # noqa: BLE001
    logger.warning(
        "samples catalog unavailable (%s) -- falling back to an inline literal DataFrame (%d row(s)).",
        exc,
        len(_txn_fallback),
    )
    base_rows = [
        row.asDict()
        for row in spark.createDataFrame(_txn_fallback, "txn_id LONG, account_id LONG, amount DOUBLE").collect()
    ]

_slice_start = (int(ITERATION) - 1) * 40
txn_rows = [
    {
        "txn_id": int(row["txn_id"]),
        "account_id": int(row["account_id"]),
        "amount": round(float(row["amount"]), 2),
        "currency": "USD",
        "txn_ts": TXN_TS,
    }
    for row in base_rows[_slice_start : _slice_start + 40]
]

_fieldnames = ["txn_id", "account_id", "amount", "currency", "txn_ts"]
_csv_buffer = io.StringIO()
_writer = csv.DictWriter(_csv_buffer, fieldnames=_fieldnames)
_writer.writeheader()
for _row in txn_rows:
    _writer.writerow(_row)
_csv_text = _csv_buffer.getvalue()

logger.info("Prepared iteration %s transaction slice: %d row(s).", ITERATION, len(txn_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build the AES-256 Password-Protected ZIP In-Process, Verify, and Land It

# COMMAND ----------

import pyzipper  # wheel dependency (pyproject.toml) -- the same library the pipeline extracts with

_zip_buffer = io.BytesIO()
with pyzipper.AESZipFile(_zip_buffer, "w", compression=pyzipper.ZIP_DEFLATED, encryption=pyzipper.WZ_AES) as _archive:
    _archive.setpassword(PASSKEY.encode("utf-8"))
    _archive.setencryption(pyzipper.WZ_AES, nbits=256)
    _archive.writestr(f"secure_txns_iter{ITERATION}.csv", _csv_text)
_zip_bytes = _zip_buffer.getvalue()

# Round-trip verification with the SAME passphrase the pipeline will resolve -- a wrong or
# stale secret value fails HERE, in the seed, not mid-pipeline-update.
with pyzipper.AESZipFile(io.BytesIO(_zip_bytes)) as _verify:
    _verify.setpassword(PASSKEY.encode("utf-8"))
    _extracted_names = _verify.namelist()
    _round_tripped = _verify.read(_extracted_names[0]).decode("utf-8")
if _round_tripped != _csv_text:
    raise RuntimeError("AES ZIP round-trip verification failed: extracted CSV differs from the built CSV.")

_incoming_dir = f"{LANDING_ROOT}/sample05_secure/incoming"
_zip_path = f"{_incoming_dir}/sample_secure_txns_iter{ITERATION}.zip"
dbutils.fs.mkdirs(_incoming_dir)
with open(_zip_path, "wb") as _destination:
    _destination.write(_zip_bytes)

logger.info(
    "Landed AES-256 password-protected archive '%s' (%d row(s), %d bytes, round-trip verified).",
    _zip_path,
    len(txn_rows),
    len(_zip_bytes),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC Iteration landed. The pipeline's `source_zip_handling` will decrypt this archive with the
# MAGIC same UC secret, extract the CSV into `sample05_secure/extracted/txns/`, and Auto Loader
# MAGIC will ingest 40 more rows into `sample_secure_txns_raw`. Re-running this notebook with the
# MAGIC same `iteration` is safe: the archive regenerates idempotently (same plaintext content,
# MAGIC overwritten each time).
