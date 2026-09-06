# Databricks notebook source
# MAGIC %md
# MAGIC # Seed ASN.1 EMSC CDR Fixture (v0.0.2)
# MAGIC
# MAGIC Provisions the two real inputs `flowx_testing/v0_0_2_asn1_emsc.json` needs:
# MAGIC
# MAGIC * The real EMSC ASN.1 module file (`BT_Usecase/UC7/data/asn_schema/EMSC.asn1`), copied
# MAGIC   verbatim into `/Volumes/{catalog}/emsc/schemas/EMSC.asn1`. Plain text, so an ordinary
# MAGIC   Workspace-Files-synced copy survives the trip fine.
# MAGIC * 10 genuine BER-encoded `CallDataRecord` fixtures, generated **directly on-cluster**
# MAGIC   (via `asn1tools`, a declared project dependency) into
# MAGIC   `/Volumes/{catalog}/emsc/landing_cdr/incoming/` -- **not** synced as pre-built binary
# MAGIC   files, since Databricks Workspace Files sync can mangle a raw `.ber` upload (same
# MAGIC   rationale as `04_seed_asn1_psgw_cdr_fixture.py`).
# MAGIC
# MAGIC ## ONE RECORD PER FILE -- the thing this notebook gets right
# MAGIC
# MAGIC `BT_Usecase/UC7/data/synthetic/emsc_synthetic.ber` holds 10 records **concatenated**
# MAGIC as 10 back-to-back TLVs in a single file. The framework decoder
# MAGIC (`asn1/decoder.py::make_partition_decoder`) calls `compiled.decode(pdu_name, raw_bytes)` on
# MAGIC the **whole file content** exactly once per Auto Loader file, and `asn1tools.decode` on a
# MAGIC concatenated buffer returns **only the first TLV, silently** -- no error, no warning.
# MAGIC Landing that one concatenated file would therefore ingest 1 row and drop 9, while
# MAGIC reporting complete success. This notebook writes each record as its **own** `.ber` file:
# MAGIC 10 files in, 10 rows out, so the row count becomes a real assertion.
# MAGIC
# MAGIC ## Root PDU auto-detection is the point
# MAGIC
# MAGIC The spec deliberately **omits** `asn1_pdu_name`, so `asn1/decoder.py::detect_root_pdu_name`
# MAGIC resolves the root PDU straight out of the module: `EMSC.asn1 -> CallDataRecord`, a 2-arm
# MAGIC **CHOICE**. The decoded frame therefore carries one nullable column per arm
# MAGIC (`uMTSGSMPLMNCallDataRecord`, `compositeCallDataRecord`) plus the synthetic `_choice`
# MAGIC discriminator naming the arm actually selected. All 10 fixtures select
# MAGIC `uMTSGSMPLMNCallDataRecord`.
# MAGIC
# MAGIC ## These records are THIN -- and the DQ rules respect that
# MAGIC
# MAGIC `UMTSGSMPLMNCallDataRecord` is a SEQUENCE whose members are **both OPTIONAL**
# MAGIC (`callDataRecord`, `eventModule`). Measured across these exact 10 deterministic records:
# MAGIC `eventModule` is present in 7/10 and `callDataRecord` in 4/10 (record 5 carries both);
# MAGIC leaf-value counts run 8-53 per record. So no individual subfield may be asserted non-null
# MAGIC -- the only honest structural claim is that **at least one** of the two members is present,
# MAGIC which does hold on all 10. The spec's `dq_config` asserts exactly that and nothing more.
# MAGIC
# MAGIC Record values are built by the deterministic generator the repo already ships
# MAGIC (`scripts/generate_synthetic_ber.py`, seeded `random.Random(SEED + index)`), reused rather
# MAGIC than reimplemented, so re-running this notebook reproduces byte-identical payloads.
# MAGIC
# MAGIC Run once per environment before onboarding `flowx_testing/v0_0_2_asn1_emsc.json`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets & Path Resolution

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_asn1_emsc_fixture")

dbutils.widgets.text("catalog", "flowx", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
SCHEMA_FIXTURE_PATH = os.path.join(REPO_ROOT, "BT_Usecase", "UC7", "data", "asn_schema", "EMSC.asn1")
GENERATOR_DIR = os.path.join(REPO_ROOT, "scripts")

if not os.path.isfile(SCHEMA_FIXTURE_PATH):
    raise FileNotFoundError(
        f"Expected EMSC ASN.1 module file at '{SCHEMA_FIXTURE_PATH}' -- is "
        "BT_Usecase/UC7/data/asn_schema/ synced alongside this notebook?"
    )

logger.info("Resolved EMSC.asn1 fixture at: %s", SCHEMA_FIXTURE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes

# COMMAND ----------

for _schema in ("emsc", "bronze_emsc"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{_schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.emsc.schemas")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.emsc.landing_cdr")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.emsc._schemas")

logger.info(
    "Provisioned catalog '%s': schemas emsc/bronze_emsc + emsc.schemas/emsc.landing_cdr/"
    "emsc._schemas volumes.",
    CATALOG,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the Real EMSC ASN.1 Module File
# MAGIC
# MAGIC Plain text -- a normal file copy is reliable here (unlike the binary CDR fixtures below).

# COMMAND ----------

EMSC_SCHEMA_VOLUME_PATH = f"/Volumes/{CATALOG}/emsc/schemas/EMSC.asn1"

dbutils.fs.cp(f"file:{SCHEMA_FIXTURE_PATH}", EMSC_SCHEMA_VOLUME_PATH)
logger.info("Landed EMSC ASN.1 module file at '%s'", EMSC_SCHEMA_VOLUME_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate the 10 BER-Encoded EMSC CDR Fixtures On-Cluster
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

ROOT_PDU = "CallDataRecord"
CHOICE_ARM = "uMTSGSMPLMNCallDataRecord"
CODEC = "ber"

EMSC_LANDING_INCOMING = f"/Volumes/{CATALOG}/emsc/landing_cdr/incoming"
dbutils.fs.mkdirs(EMSC_LANDING_INCOMING)

compiled = asn1tools.compile_files([EMSC_SCHEMA_VOLUME_PATH], CODEC)

# build_records() resolves the schema out of BT_Usecase/UC7/data/asn_schema/ by name and returns
# 10 deterministic (arm_name, value) tuples for the root CHOICE.
records, _index, _chosen = build_records("EMSC.asn1", ROOT_PDU, CHOICE_ARM)
logger.info("Built %d deterministic %s/%s records", len(records), ROOT_PDU, CHOICE_ARM)

written = 0
for _i, _record in enumerate(records):
    # check_constraints=False mirrors the generator: these real telecom schemas carry
    # constraints the synthetic values do not all satisfy, and the framework decoder does not
    # enforce them on read either.
    _encoded = compiled.encode(ROOT_PDU, _record, check_constraints=False)
    _output_path = f"{EMSC_LANDING_INCOMING}/emsc_cdr_{_i:03d}.ber"
    with open(_output_path, "wb") as _handle:
        _handle.write(_encoded)
    written += 1
    logger.info("Wrote BER-encoded EMSC CDR fixture '%s' (%d bytes)", _output_path, len(_encoded))

logger.info("Wrote %d single-record BER files into %s", written, EMSC_LANDING_INCOMING)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Self-Check: Every File Decodes, Holds Exactly One Record, and Is Non-Empty
# MAGIC
# MAGIC Guards the one-record-per-file invariant at seed time rather than letting a silent
# MAGIC truncation surface later as a quietly-wrong row count. The last check -- that the
# MAGIC selected arm carries at least one of its two OPTIONAL members -- is exactly the claim
# MAGIC the spec's `dq_emsc_arm_has_content` rule makes, verified here before the pipeline can
# MAGIC false-quarantine on it. Per-member counts are logged but deliberately NOT asserted:
# MAGIC neither member is present in all 10 records.

# COMMAND ----------

_expected = len(records)
_decoded_ok = 0
_arms = set()
_member_presence = {"callDataRecord": 0, "eventModule": 0}

for _i in range(_expected):
    _path = f"{EMSC_LANDING_INCOMING}/emsc_cdr_{_i:03d}.ber"
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

    # Both members of the arm are OPTIONAL, so assert only what is actually true of all 10:
    # at least one of them is present.
    _value = _decoded[1]
    if not isinstance(_value, dict) or not _value:
        raise AssertionError(f"{_path}: arm {_decoded[0]} decoded empty -- {_value!r}")
    for _member in _member_presence:
        if _value.get(_member) is not None:
            _member_presence[_member] += 1

    _decoded_ok += 1

if _decoded_ok != _expected:
    raise AssertionError(f"Expected {_expected} decodable fixtures, got {_decoded_ok}")

logger.info(
    "Self-check PASSED: %d/%d single-record BER files decode cleanly; CHOICE arm(s) selected: %s",
    _decoded_ok,
    _expected,
    sorted(_arms),
)
logger.info(
    "OPTIONAL member presence across the %d records (informational, NOT asserted): %s",
    _expected,
    _member_presence,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `flowx_testing/v0_0_2_asn1_emsc.json` can now be onboarded and its pipeline run.
# MAGIC Expect **10 rows** in `bronze_emsc.emsc_cdr_raw`, every row carrying
# MAGIC `_choice = 'uMTSGSMPLMNCallDataRecord'` and `_asn1_decode_error IS NULL`, with
# MAGIC `compositeCallDataRecord` NULL throughout.
# MAGIC
# MAGIC Re-running this notebook is safe: the schema file is re-copied (overwritten) and the 10
# MAGIC CDR fixtures regenerate with identical content -- Auto Loader itself, not this notebook,
# MAGIC tracks which files it has already ingested via its checkpoint.
