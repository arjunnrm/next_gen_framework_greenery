# Databricks notebook source
# MAGIC %md
# MAGIC # Seed ASN.1 GGSN CDR Fixture (v0.0.2)
# MAGIC
# MAGIC Provisions the two real inputs `flowx_testing/v0_0_2_asn1_ggsn.json` needs:
# MAGIC
# MAGIC * The real GGSN ASN.1 module file (`flowx_testing/BT_Testing/GGSN.asn1`), copied
# MAGIC   verbatim into `/Volumes/{catalog}/ggsn/schemas/GGSN.asn1`. Plain text, so an ordinary
# MAGIC   Workspace-Files-synced copy survives the trip fine.
# MAGIC * 10 genuine BER-encoded `CallEventRecord` fixtures, generated **directly on-cluster**
# MAGIC   (via `asn1tools`, a declared project dependency) into
# MAGIC   `/Volumes/{catalog}/ggsn/landing_cdr/incoming/` -- **not** synced as pre-built binary
# MAGIC   files, since Databricks Workspace Files sync can mangle a raw `.ber` upload.
# MAGIC
# MAGIC ## ONE RECORD PER FILE -- the thing this notebook gets right
# MAGIC
# MAGIC `flowx_testing/BT_Testing/synthetic/ggsn_synthetic.ber` holds its 10 records
# MAGIC **concatenated** as 10 back-to-back TLVs in a single file. The framework decoder
# MAGIC (`asn1/decoder.py::make_partition_decoder`) calls `compiled.decode(pdu_name, raw_bytes)` on
# MAGIC the **whole file content** exactly once per Auto Loader file, and `asn1tools.decode` on a
# MAGIC concatenated buffer returns **only the first TLV, silently** -- no error, no warning.
# MAGIC Landing that one concatenated file would therefore ingest 1 row and drop 9, while
# MAGIC reporting complete success. This notebook writes each record as its **own** `.ber` file:
# MAGIC 10 files in, 10 rows out, and the row count becomes a real assertion.
# MAGIC
# MAGIC ## Why GGSN / `sgsnMMRecord`
# MAGIC
# MAGIC GGSN's root PDU `CallEventRecord` is a **CHOICE** (13 arms), so it exercises the v0.0.2
# MAGIC CHOICE-root support: the decoded frame carries one column per arm plus the `_choice`
# MAGIC discriminator (14 columns total). `sgsnMMRecord` is the arm the repo's deterministic
# MAGIC generator selects for GGSN -- 9-21 populated leaf values per record across these 10.
# MAGIC
# MAGIC The spec deliberately **omits** `asn1_pdu_name` so the framework's own
# MAGIC `asn1/decoder.py::detect_root_pdu_name` resolves the root PDU from the module itself
# MAGIC (`GGSN.asn1` -> `CallEventRecord`). That auto-detection is the headline v0.0.2 capability
# MAGIC this fixture exists to exercise.
# MAGIC
# MAGIC Record values are built by the deterministic generator the repo already ships
# MAGIC (`scripts/generate_synthetic_ber.py`, seeded `random.Random(SEED + index)`), reused rather
# MAGIC than reimplemented, so re-running this notebook reproduces byte-identical payloads.
# MAGIC
# MAGIC Run once per environment before onboarding `flowx_testing/v0_0_2_asn1_ggsn.json`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets & Path Resolution

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_asn1_ggsn_fixture")

dbutils.widgets.text("catalog", "flowx", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
SCHEMA_FIXTURE_PATH = os.path.join(REPO_ROOT, "flowx_testing", "BT_Testing", "GGSN.asn1")
GENERATOR_DIR = os.path.join(REPO_ROOT, "scripts")

if not os.path.isfile(SCHEMA_FIXTURE_PATH):
    raise FileNotFoundError(
        f"Expected GGSN ASN.1 module file at '{SCHEMA_FIXTURE_PATH}' -- is "
        "flowx_testing/BT_Testing/ synced alongside this notebook?"
    )

logger.info("Resolved GGSN.asn1 fixture at: %s", SCHEMA_FIXTURE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes
# MAGIC
# MAGIC All GGSN-specific -- nothing here is shared with the PSGW (TC1) fixture or with the
# MAGIC EMSC/TAP.310/TAP.311 fixtures, so the four protocol pipelines never collide.

# COMMAND ----------

for _schema in ("ggsn", "bronze_ggsn"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{_schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.ggsn.schemas")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.ggsn.landing_cdr")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.ggsn._schemas")

logger.info(
    "Provisioned catalog '%s': schemas ggsn/bronze_ggsn + ggsn.schemas/ggsn.landing_cdr/"
    "ggsn._schemas volumes.",
    CATALOG,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the Real GGSN ASN.1 Module File
# MAGIC
# MAGIC Plain text -- a normal file copy is reliable here (unlike the binary CDR fixtures below).

# COMMAND ----------

GGSN_SCHEMA_VOLUME_PATH = f"/Volumes/{CATALOG}/ggsn/schemas/GGSN.asn1"

dbutils.fs.cp(f"file:{SCHEMA_FIXTURE_PATH}", GGSN_SCHEMA_VOLUME_PATH)
logger.info("Landed GGSN ASN.1 module file at '%s'", GGSN_SCHEMA_VOLUME_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate the 10 BER-Encoded GGSN CDR Fixtures On-Cluster
# MAGIC
# MAGIC Compiled straight from the module file just landed above, so the compiled schema identity
# MAGIC matches exactly what the pipeline's own `asn1_schema_path` points at. Record *values* come
# MAGIC from the repo's existing deterministic builder, reused rather than reimplemented.

# COMMAND ----------

import sys

if GENERATOR_DIR not in sys.path:
    sys.path.insert(0, GENERATOR_DIR)

import asn1tools
from generate_synthetic_ber import build_records  # noqa: E402  (sys.path set up immediately above)

ROOT_PDU = "CallEventRecord"
CHOICE_ARM = "sgsnMMRecord"
CODEC = "ber"

GGSN_LANDING_INCOMING = f"/Volumes/{CATALOG}/ggsn/landing_cdr/incoming"
dbutils.fs.mkdirs(GGSN_LANDING_INCOMING)

compiled = asn1tools.compile_files([GGSN_SCHEMA_VOLUME_PATH], CODEC)

# build_records() resolves the schema out of flowx_testing/BT_Testing/ by name and returns
# 10 deterministic (arm_name, value) tuples for the root CHOICE.
records, _index, _chosen = build_records("GGSN.asn1", ROOT_PDU, CHOICE_ARM)
logger.info("Built %d deterministic %s/%s records", len(records), ROOT_PDU, CHOICE_ARM)

written = 0
for _i, _record in enumerate(records):
    # check_constraints=False mirrors the generator: these real telecom schemas carry
    # constraints the synthetic values do not all satisfy, and the framework decoder does not
    # enforce them on read either.
    _encoded = compiled.encode(ROOT_PDU, _record, check_constraints=False)
    _output_path = f"{GGSN_LANDING_INCOMING}/ggsn_cdr_{_i:03d}.ber"
    with open(_output_path, "wb") as _handle:
        _handle.write(_encoded)
    written += 1
    logger.info("Wrote BER-encoded GGSN CDR fixture '%s' (%d bytes)", _output_path, len(_encoded))

logger.info("Wrote %d single-record BER files into %s", written, GGSN_LANDING_INCOMING)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Self-Check: Every File Decodes, and Holds Exactly One Record
# MAGIC
# MAGIC Guards the one-record-per-file invariant at seed time rather than letting a silent
# MAGIC truncation surface later as a quietly-wrong row count. Also asserts the five members
# MAGIC that are genuinely populated in **all 10** of these records -- the exact set the spec's
# MAGIC `dq_ggsn_mandatory_fields_present` rule asserts on, verified here at the source rather
# MAGIC than assumed.

# COMMAND ----------

# Verified across all 10 generated records (see the spec's _expected_result): every other
# sgsnMMRecord member is OPTIONAL and populated in only 2-6 of the 10, so asserting on it
# would be dishonest. duration, notably, appears in just 2/10.
ALWAYS_PRESENT_MEMBERS = (
    "recordType",
    "servedIMSI",
    "recordOpeningTime",
    "causeForRecClosing",
    "chargingCharacteristics",
)

_expected = len(records)
_decoded_ok = 0
_arms = set()

for _i in range(_expected):
    _path = f"{GGSN_LANDING_INCOMING}/ggsn_cdr_{_i:03d}.ber"
    with open(_path, "rb") as _handle:
        _raw = _handle.read()

    _decoded = compiled.decode(ROOT_PDU, _raw)
    if not (isinstance(_decoded, tuple) and len(_decoded) == 2 and _decoded[0] is not None):
        raise AssertionError(f"{_path}: CHOICE matched no arm -- got {_decoded!r}")
    _arms.add(_decoded[0])

    # The whole file must be exactly one TLV: re-encoding the decode must consume every byte.
    # A concatenated multi-record file decodes its first TLV happily and drops the rest.
    _reencoded = compiled.encode(ROOT_PDU, _decoded, check_constraints=False)
    if len(_reencoded) != len(_raw):
        raise AssertionError(
            f"{_path}: file is {len(_raw)} bytes but its first TLV is {len(_reencoded)} -- the "
            "file holds more than one record, which the framework decoder would silently truncate."
        )

    _body = _decoded[1]
    _missing = [_m for _m in ALWAYS_PRESENT_MEMBERS if _body.get(_m) is None]
    if _missing:
        raise AssertionError(
            f"{_path}: expected always-present sgsnMMRecord member(s) {_missing} are absent -- "
            "the spec's dq_ggsn_mandatory_fields_present rule would quarantine this record."
        )
    _decoded_ok += 1

if _decoded_ok != _expected:
    raise AssertionError(f"Expected {_expected} decodable fixtures, got {_decoded_ok}")

logger.info(
    "Self-check PASSED: %d/%d single-record BER files decode cleanly; CHOICE arm(s) selected: %s; "
    "all five always-present members non-null on every record.",
    _decoded_ok,
    _expected,
    sorted(_arms),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `flowx_testing/v0_0_2_asn1_ggsn.json` can now be onboarded and its pipeline run.
# MAGIC Expect **10 rows** in `bronze_ggsn.ggsn_cdr_raw`, every row carrying
# MAGIC `_choice = 'sgsnMMRecord'` and `_asn1_decode_error IS NULL`.
# MAGIC
# MAGIC Re-running this notebook is safe: the schema file is re-copied (overwritten) and the 10
# MAGIC CDR fixtures regenerate with identical content -- Auto Loader itself, not this notebook,
# MAGIC tracks which files it has already ingested via its checkpoint.
