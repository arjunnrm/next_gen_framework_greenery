# Databricks notebook source
# MAGIC %md
# MAGIC # Seed ASN.1 PSGW CDR Fixture (v0.0.2 TC1)
# MAGIC
# MAGIC Provisions the two real inputs `metaflow_testing/v0_0_2_tc1_asn1_ingest.json` needs:
# MAGIC
# MAGIC * The real PSGW ASN.1 module file (`metaflow_testing/BT_Testing/PSGW.asn1`), copied
# MAGIC   verbatim into `/Volumes/{catalog}/psgw/schemas/PSGW.asn1`. Plain text, so an ordinary
# MAGIC   Workspace-Files-synced copy survives the trip fine.
# MAGIC * 10 genuine BER-encoded `CallEventRecord` fixtures, generated **directly on-cluster**
# MAGIC   (via `asn1tools`, a declared project dependency) into
# MAGIC   `/Volumes/{catalog}/psgw/landing_cdr/incoming/` -- **not** synced as pre-built binary
# MAGIC   files, since Databricks Workspace Files sync can mangle a raw `.ber` upload (same
# MAGIC   rationale as `03_seed_asn1_gsm_cdr_fixture.py`).
# MAGIC
# MAGIC ## ONE RECORD PER FILE -- the thing this notebook gets right
# MAGIC
# MAGIC `metaflow_testing/BT_Testing/synthetic/psgw_synthetic.ber` holds 10 records **concatenated**
# MAGIC as 10 back-to-back TLVs in a single file. The framework decoder
# MAGIC (`asn1/decoder.py::make_partition_decoder`) calls `compiled.decode(pdu_name, raw_bytes)` on
# MAGIC the **whole file content** exactly once per Auto Loader file, and `asn1tools.decode` on a
# MAGIC concatenated buffer returns **only the first TLV, silently** -- no error, no warning.
# MAGIC Landing that one concatenated file would therefore ingest 1 row and drop 9, while
# MAGIC reporting complete success. That is precisely the silent truncation this test case exists
# MAGIC to disprove, so this notebook writes each record as its **own** `.ber` file:
# MAGIC 10 files in, 10 rows out, and the row count becomes a real assertion.
# MAGIC
# MAGIC ## Why PSGW / `pGWRecord`
# MAGIC
# MAGIC PSGW's root PDU `CallEventRecord` is a **CHOICE** (13 arms), so it exercises the v0.0.2
# MAGIC CHOICE-root support: the decoded frame carries one column per arm plus the `_choice`
# MAGIC discriminator. `pGWRecord` is the richest arm -- 18-24 populated leaf values per record
# MAGIC across these 10 -- which is what makes the schema-mapping half of the test meaningful.
# MAGIC
# MAGIC Record values are built by the deterministic generator the repo already ships
# MAGIC (`scripts/generate_synthetic_ber.py`, seeded `random.Random(SEED + index)`), reused rather
# MAGIC than reimplemented, so re-running this notebook reproduces byte-identical payloads.
# MAGIC
# MAGIC Run once per environment before onboarding `metaflow_testing/v0_0_2_tc1_asn1_ingest.json`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets & Path Resolution

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_asn1_psgw_cdr_fixture")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
SCHEMA_FIXTURE_PATH = os.path.join(REPO_ROOT, "metaflow_testing", "BT_Testing", "PSGW.asn1")
GENERATOR_DIR = os.path.join(REPO_ROOT, "scripts")

if not os.path.isfile(SCHEMA_FIXTURE_PATH):
    raise FileNotFoundError(
        f"Expected PSGW ASN.1 module file at '{SCHEMA_FIXTURE_PATH}' -- is "
        "metaflow_testing/BT_Testing/ synced alongside this notebook?"
    )

logger.info("Resolved PSGW.asn1 fixture at: %s", SCHEMA_FIXTURE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes

# COMMAND ----------

for _schema in ("psgw", "bronze_psgw"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{_schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.psgw.schemas")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.psgw.landing_cdr")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.psgw._schemas")

logger.info(
    "Provisioned catalog '%s': schemas psgw/bronze_psgw + psgw.schemas/psgw.landing_cdr/"
    "psgw._schemas volumes.",
    CATALOG,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the Real PSGW ASN.1 Module File
# MAGIC
# MAGIC Plain text -- a normal file copy is reliable here (unlike the binary CDR fixtures below).

# COMMAND ----------

PSGW_SCHEMA_VOLUME_PATH = f"/Volumes/{CATALOG}/psgw/schemas/PSGW.asn1"

dbutils.fs.cp(f"file:{SCHEMA_FIXTURE_PATH}", PSGW_SCHEMA_VOLUME_PATH)
logger.info("Landed PSGW ASN.1 module file at '%s'", PSGW_SCHEMA_VOLUME_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate the 10 BER-Encoded PSGW CDR Fixtures On-Cluster
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
CHOICE_ARM = "pGWRecord"
CODEC = "ber"

PSGW_LANDING_INCOMING = f"/Volumes/{CATALOG}/psgw/landing_cdr/incoming"
dbutils.fs.mkdirs(PSGW_LANDING_INCOMING)

compiled = asn1tools.compile_files([PSGW_SCHEMA_VOLUME_PATH], CODEC)

# build_records() resolves the schema out of metaflow_testing/BT_Testing/ by name and returns
# 10 deterministic (arm_name, value) tuples for the root CHOICE.
records, _index, _chosen = build_records("PSGW.asn1", ROOT_PDU, CHOICE_ARM)
logger.info("Built %d deterministic %s/%s records", len(records), ROOT_PDU, CHOICE_ARM)

written = 0
for _i, _record in enumerate(records):
    # check_constraints=False mirrors the generator: these real telecom schemas carry
    # constraints the synthetic values do not all satisfy, and the framework decoder does not
    # enforce them on read either.
    _encoded = compiled.encode(ROOT_PDU, _record, check_constraints=False)
    _output_path = f"{PSGW_LANDING_INCOMING}/psgw_cdr_{_i:03d}.ber"
    with open(_output_path, "wb") as _handle:
        _handle.write(_encoded)
    written += 1
    logger.info("Wrote BER-encoded PSGW CDR fixture '%s' (%d bytes)", _output_path, len(_encoded))

logger.info("Wrote %d single-record BER files into %s", written, PSGW_LANDING_INCOMING)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Self-Check: Every File Decodes, and Holds Exactly One Record
# MAGIC
# MAGIC Guards the one-record-per-file invariant at seed time rather than letting a silent
# MAGIC truncation surface later as a quietly-wrong row count.

# COMMAND ----------

_expected = len(records)
_decoded_ok = 0
_arms = set()

for _i in range(_expected):
    _path = f"{PSGW_LANDING_INCOMING}/psgw_cdr_{_i:03d}.ber"
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
    _decoded_ok += 1

if _decoded_ok != _expected:
    raise AssertionError(f"Expected {_expected} decodable fixtures, got {_decoded_ok}")

logger.info(
    "Self-check PASSED: %d/%d single-record BER files decode cleanly; CHOICE arm(s) selected: %s",
    _decoded_ok,
    _expected,
    sorted(_arms),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `metaflow_testing/v0_0_2_tc1_asn1_ingest.json` can now be onboarded and its pipeline run.
# MAGIC Expect **10 rows** in `bronze_psgw.psgw_cdr_raw`, every row carrying
# MAGIC `_choice = 'pGWRecord'` and `_asn1_decode_error IS NULL`.
# MAGIC
# MAGIC Re-running this notebook is safe: the schema file is re-copied (overwritten) and the 10
# MAGIC CDR fixtures regenerate with identical content -- Auto Loader itself, not this notebook,
# MAGIC tracks which files it has already ingested via its checkpoint.
