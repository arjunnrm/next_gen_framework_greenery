# Databricks notebook source
# MAGIC %md
# MAGIC # Seed Sample 06 -- Real GSMA TAP3 ASN.1 BER Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `resources/sample_jobs/onboarding/sample_06_asn1_tap3_ingestion.json`
# MAGIC only. Parameterized by `iteration`; the common seed job
# MAGIC (`resources/sample_jobs/metaflow_sample_seed_job.yml`) invokes it once per iteration
# MAGIC (1 -> 2 -> 3, chained) before any sample pipeline runs.
# MAGIC
# MAGIC Unlike every other seed in this suite there is no `samples`-catalog slice to take: a TAP3
# MAGIC record is a wire-format telecom document, not a table. The fixtures are therefore
# MAGIC generated deterministically from literals -- the same `iteration` always produces the
# MAGIC same bytes.
# MAGIC
# MAGIC ## The two things this notebook provisions
# MAGIC
# MAGIC 1. **The real ASN.1 module.** `metaflow_testing/BT_Testing/TAP.310.asn1` -- the genuine
# MAGIC    GSMA TAP release 3.10 specification already shipped in this repo, 1597 lines, 375
# MAGIC    types -- copied verbatim into
# MAGIC    `/Volumes/{catalog}/metaflow_sample/landing/sample06_asn1/schemas/TAP.310.asn1`. Plain
# MAGIC    text, so an ordinary Workspace-Files-synced copy survives the trip fine (identical
# MAGIC    rationale to `03_seed_asn1_gsm_cdr_fixture.py`).
# MAGIC 2. **BER-encoded `Notification` fixtures**, generated **directly on-cluster** with
# MAGIC    `asn1tools` (a declared project dependency -- see `pyproject.toml`) straight into
# MAGIC    `/Volumes/{catalog}/metaflow_sample/landing/sample06_asn1/incoming/` -- **never**
# MAGIC    synced as pre-built binary files, since Databricks Workspace Files sync can mangle a
# MAGIC    raw `.ber` upload. The module is compiled from the copy landed in step 1, so the
# MAGIC    compiled schema identity the seed encodes with is byte-identical to the one the
# MAGIC    pipeline's own `asn1_schema_path` points at.
# MAGIC
# MAGIC ## Why `Notification` and not `DataInterChange`
# MAGIC
# MAGIC TAP.310's top-level `DataInterChange` is an ASN.1 `CHOICE`, and its `TransferBatch` arm
# MAGIC reaches `CallEventDetail`, also a `CHOICE`.
# MAGIC `asn1/decoder.py::derive_asn1_field_defs` supports neither (it raises
# MAGIC `Asn1DecodeError: ASN.1 CHOICE types are not yet supported ...`) and requires the PDU to
# MAGIC be a top-level `SEQUENCE`. `Notification` is `[APPLICATION 2] SEQUENCE`, its whole member
# MAGIC tree resolves, and it is a real TAP3 file-level PDU -- the notification file a roaming
# MAGIC partner sends when it has no chargeable events to transfer. It also exercises three
# MAGIC distinct Spark output shapes in one PDU: scalars, a nested `SEQUENCE` -> `struct`
# MAGIC (`DateTimeLong`), and a `SEQUENCE OF` -> `array` (`operatorSpecInformation`).
# MAGIC
# MAGIC ## Deliberate decode failures
# MAGIC
# MAGIC Each iteration also lands 2 **truncated** `.ber` payloads (a valid encoding cut mid-TLV).
# MAGIC They decode-fail per row, so `asn1/decoder.py` populates `_asn1_decode_error` instead of
# MAGIC aborting the micro-batch, and the spec's `asn1_decode_ok` quarantine rule routes them
# MAGIC into `sample_tap3_notification_raw_quarantine`.
# MAGIC
# MAGIC Idempotent: re-running an iteration overwrites the same filenames with the same
# MAGIC deterministic bytes. Auto Loader itself, not this notebook, tracks which files it has
# MAGIC already ingested.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets & Path Resolution

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_sample_06_asn1_tap3")

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
SAMPLE06_ROOT = f"{LANDING_ROOT}/sample06_asn1"
SCHEMA_DIR = f"{SAMPLE06_ROOT}/schemas"
INCOMING_DIR = f"{SAMPLE06_ROOT}/incoming"
LANDED_MODULE_PATH = f"{SCHEMA_DIR}/TAP.310.asn1"

# The repo-relative location of the real GSMA module. Resolved from this notebook's own
# location first (the reliable answer under a bundle-synced notebook task) with getcwd() as a
# fallback, exactly as 03_seed_asn1_gsm_cdr_fixture.py does; several candidate roots are tried
# because a notebook's working directory is not guaranteed to be its own directory.
_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
_MODULE_RELATIVE_PATH = os.path.join("metaflow_testing", "BT_Testing", "TAP.310.asn1")
_candidate_roots = [
    os.path.abspath(os.path.join(_this_dir, "..", "..")),
    os.path.abspath(os.path.join(os.getcwd(), "..", "..")),
    os.getcwd(),
]

SOURCE_MODULE_PATH = None
for _root in _candidate_roots:
    _candidate = os.path.join(_root, _MODULE_RELATIVE_PATH)
    if os.path.isfile(_candidate):
        SOURCE_MODULE_PATH = _candidate
        break

if SOURCE_MODULE_PATH is None:
    raise FileNotFoundError(
        f"Expected the GSMA TAP release 3.10 module at '{_MODULE_RELATIVE_PATH}' under one of "
        f"{_candidate_roots} -- is metaflow_testing/BT_Testing/ synced alongside this notebook?"
    )

logger.info("Resolved TAP.310.asn1 at: %s", SOURCE_MODULE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision the Single Sample Schema & Landing Volume

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SAMPLE_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SAMPLE_SCHEMA}.landing")
dbutils.fs.mkdirs(SCHEMA_DIR)
dbutils.fs.mkdirs(INCOMING_DIR)

logger.info("Provisioned %s.%s with the landing volume (%s).", CATALOG, SAMPLE_SCHEMA, SAMPLE06_ROOT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the Real GSMA TAP.310 Module File
# MAGIC
# MAGIC Plain text -- a normal file copy is reliable here (unlike the binary fixtures below).

# COMMAND ----------

dbutils.fs.cp(f"file:{SOURCE_MODULE_PATH}", LANDED_MODULE_PATH)
logger.info(
    "Landed ASN.1 module '%s' -> '%s' (%d bytes).",
    SOURCE_MODULE_PATH,
    LANDED_MODULE_PATH,
    os.path.getsize(SOURCE_MODULE_PATH),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build This Iteration's Deterministic TAP3 Notification Records
# MAGIC
# MAGIC 6 valid records per iteration, sequence numbers `00001`..`00018` across the three
# MAGIC iterations, so nothing collides and every iteration's contribution is identifiable by
# MAGIC `fileSequenceNumber` alone.

# COMMAND ----------

_ITERATION_INDEX = int(ITERATION)
_RECORDS_PER_ITERATION = 6

# TADIG codes are 5 characters (PlmnId ::= AsciiString, SIZE(5) per the module's own comment):
# 3-letter country + 2-letter operator. One sender, a rotating set of roaming partners.
_SENDER_PLMN = "GBRBT"
_RECIPIENT_PLMNS = ["DEUD1", "FRAF1", "ESPTE", "ITAOM", "USACG", "JPNDO"]
_FILE_DATE = {"1": "20260901", "2": "20260902", "3": "20260903"}[ITERATION]


def _date_time_long(hhmmss: str) -> dict:
    """One TAP3 ``DateTimeLong`` -- decodes to a Spark ``struct<localTimeStamp,utcTimeOffset>``."""
    return {"localTimeStamp": f"{_FILE_DATE}{hhmmss}", "utcTimeOffset": "+0000"}


notification_records = []
for _offset in range(_RECORDS_PER_ITERATION):
    _sequence = (_ITERATION_INDEX - 1) * _RECORDS_PER_ITERATION + _offset + 1
    notification_records.append(
        {
            "sender": _SENDER_PLMN,
            "recipient": _RECIPIENT_PLMNS[_offset % len(_RECIPIENT_PLMNS)],
            "fileSequenceNumber": f"{_sequence:05d}",
            "fileCreationTimeStamp": _date_time_long(f"{6 + _offset:02d}0000"),
            "fileAvailableTimeStamp": _date_time_long(f"{6 + _offset:02d}3000"),
            "transferCutOffTimeStamp": _date_time_long(f"{5 + _offset:02d}0000"),
            "specificationVersionNumber": 3,
            "releaseVersionNumber": 10,
            "fileTypeIndicator": "T",
            "operatorSpecInformation": [
                f"METAFLOW-SAMPLE-06 ITERATION {ITERATION}",
                f"NOTIFICATION SEQ {_sequence:05d}",
            ],
        }
    )

logger.info("Prepared %d TAP3 Notification record(s) for iteration %s.", len(notification_records), ITERATION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. BER-Encode On-Cluster and Land the `.ber` Payloads
# MAGIC
# MAGIC Compiled once, from the module file landed in step 2 -- not from the repo copy -- so the
# MAGIC encoding schema and the pipeline's `asn1_schema_path` are provably the same document.
# MAGIC Every encoding is round-trip-decoded before it is written: an unreadable fixture must
# MAGIC fail here, in the seed, not silently three tasks later as a quarantined row.

# COMMAND ----------

import asn1tools

compiled = asn1tools.compile_files(LANDED_MODULE_PATH, "ber")

_written = 0
_last_encoded = None
for _record in notification_records:
    _encoded = compiled.encode("Notification", _record)
    _decoded = compiled.decode("Notification", _encoded)
    if _decoded.get("fileSequenceNumber") != _record["fileSequenceNumber"]:
        raise ValueError(
            f"Round-trip check failed for Notification {_record['fileSequenceNumber']}: "
            f"decoded fileSequenceNumber is {_decoded.get('fileSequenceNumber')!r}."
        )
    _output_path = f"{INCOMING_DIR}/tap3_notification_iter{ITERATION}_{_record['fileSequenceNumber']}.ber"
    with open(_output_path, "wb") as _handle:
        _handle.write(_encoded)
    logger.info("Wrote BER Notification '%s' (%d bytes).", _output_path, len(_encoded))
    _last_encoded = _encoded
    _written += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Land 2 Deliberately Truncated Payloads per Iteration
# MAGIC
# MAGIC A BER payload cut mid-TLV: `asn1tools` raises during `decode`, `asn1/decoder.py` captures
# MAGIC that per row into `_asn1_decode_error` rather than failing the whole micro-batch, and the
# MAGIC spec's `asn1_decode_ok` rule quarantines the row. `fileSequenceNumber` -- the quarantine
# MAGIC table's `record_id_column` -- is NULL on these rows by construction: nothing decoded.

# COMMAND ----------

_truncation_ratios = (0.4, 0.25)
for _index, _ratio in enumerate(_truncation_ratios):
    _truncated = _last_encoded[: max(2, int(len(_last_encoded) * _ratio))]
    _output_path = f"{INCOMING_DIR}/tap3_notification_iter{ITERATION}_corrupt{_index + 1}.ber"
    with open(_output_path, "wb") as _handle:
        _handle.write(_truncated)
    logger.info("Wrote truncated (undecodable) payload '%s' (%d bytes).", _output_path, len(_truncated))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC Iteration landed: 6 decodable `Notification` payloads + 2 truncated ones. Expected in
# MAGIC `sample_tap3_notification_raw` once all three iterations are seeded and the pipeline runs:
# MAGIC **18 clean rows** (`fileSequenceNumber` `00001`..`00018`) and **6 quarantined rows** in
# MAGIC `sample_tap3_notification_raw_quarantine`, each carrying a populated `_asn1_decode_error`.

# COMMAND ----------

logger.info(
    "Iteration %s complete: %d valid + %d truncated payload(s) under '%s'.",
    ITERATION,
    _written,
    len(_truncation_ratios),
    INCOMING_DIR,
)
