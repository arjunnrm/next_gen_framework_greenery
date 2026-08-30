# Databricks notebook source
# MAGIC %md
# MAGIC # Seed ASN.1 GSM CDR Fixture (TC-ING-004)
# MAGIC
# MAGIC Provisions the two real inputs `metaflow_testing/013_ing_004_asn1_decode.json` needs:
# MAGIC
# MAGIC * The real ASN.1 module file (`sample_data/asn1_schema/gsm_cdr.asn`), copied verbatim
# MAGIC   into `/Volumes/{catalog}/telecom/schemas/gsm_cdr.asn` -- a plain text file, so an
# MAGIC   ordinary Workspace-Files-synced copy survives the trip fine.
# MAGIC * 3 genuine BER-encoded `GsmCallDetailRecord` fixtures, generated **directly on-cluster**
# MAGIC   (via `asn1tools`, already a declared project dependency -- see pyproject.toml) straight
# MAGIC   into `/Volumes/{catalog}/telecom/landing_cdr/incoming/` -- **not** synced as pre-built
# MAGIC   binary files, since Databricks Workspace Files sync can mangle a raw `.ber` upload
# MAGIC   (identical rationale to `sample_data/generate_asn1_dq_fixtures.py`'s note, and
# MAGIC   `02_seed_metaflow_testing_data.py`'s own ZIP-archive generation).
# MAGIC * All 3 records are valid, decodable `GsmCallDetailRecord` instances -- TC-ING-004 proves
# MAGIC   a clean decode (`imsi`/`callDurationSeconds` populated, zero `_asn1_decode_error` rows),
# MAGIC   not the decode-failure path (already covered by `docs/13_asn1_dq_quarantine.md`).
# MAGIC
# MAGIC Run once per environment before onboarding `metaflow_testing/013_ing_004_asn1_decode.json`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets & Path Resolution

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_asn1_gsm_cdr_fixture")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
SCHEMA_FIXTURE_PATH = os.path.join(REPO_ROOT, "sample_data", "asn1_schema", "gsm_cdr.asn")

if not os.path.isfile(SCHEMA_FIXTURE_PATH):
    raise FileNotFoundError(
        f"Expected ASN.1 module file at '{SCHEMA_FIXTURE_PATH}' -- is sample_data/ synced alongside this notebook?"
    )

logger.info("Resolved gsm_cdr.asn fixture at: %s", SCHEMA_FIXTURE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes

# COMMAND ----------

for _schema in ("telecom", "bronze_telecom"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{_schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.telecom.schemas")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.telecom.landing_cdr")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.telecom._schemas")

logger.info(
    "Provisioned catalog '%s': schemas telecom/bronze_telecom + telecom.schemas/telecom.landing_cdr/"
    "telecom._schemas volumes.",
    CATALOG,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the Real ASN.1 Module File
# MAGIC
# MAGIC Plain text -- a normal file copy is reliable here (unlike the binary CDR fixtures below).

# COMMAND ----------

TELECOM_SCHEMA_VOLUME_PATH = f"/Volumes/{CATALOG}/telecom/schemas/gsm_cdr.asn"

dbutils.fs.cp(f"file:{SCHEMA_FIXTURE_PATH}", TELECOM_SCHEMA_VOLUME_PATH)
logger.info("Landed ASN.1 module file at '%s'", TELECOM_SCHEMA_VOLUME_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate the 3 BER-Encoded GSM CDR Fixtures On-Cluster
# MAGIC
# MAGIC Compiled once, straight from the module file just landed above (so the compiled schema
# MAGIC identity matches exactly what the pipeline's own `asn1_schema_path` will point at), then
# MAGIC encoded and written directly as raw bytes into the landing Volume.

# COMMAND ----------

import asn1tools

CDR_LANDING_INCOMING = f"/Volumes/{CATALOG}/telecom/landing_cdr/incoming"
dbutils.fs.mkdirs(CDR_LANDING_INCOMING)

compiled = asn1tools.compile_files(TELECOM_SCHEMA_VOLUME_PATH, "ber")

_records = {
    "gsm_cdr_001.ber": {
        "imsi": "310150555000001",
        "imei": "490154203237518",
        "callDurationSeconds": 180,
        "cellId": "CELL-GSM-001",
        "roamingFlag": False,
    },
    "gsm_cdr_002.ber": {
        "imsi": "310150555000002",
        "imei": "490154203237519",
        "callDurationSeconds": 42,
        "cellId": "CELL-GSM-002",
        "roamingFlag": True,
    },
    "gsm_cdr_003.ber": {
        "imsi": "310150555000003",
        "imei": "490154203237520",
        "callDurationSeconds": 305,
        "cellId": "CELL-GSM-003",
        "roamingFlag": False,
    },
}

for _filename, _record in _records.items():
    _encoded = compiled.encode("GsmCallDetailRecord", _record)
    _output_path = f"{CDR_LANDING_INCOMING}/{_filename}"
    with open(_output_path, "wb") as _handle:
        _handle.write(_encoded)
    logger.info("Wrote BER-encoded CDR fixture '%s' (%d bytes)", _output_path, len(_encoded))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `metaflow_testing/013_ing_004_asn1_decode.json` can now be onboarded and its pipeline run.
# MAGIC Re-running this notebook is safe: the schema file is re-copied (overwritten) and the 3
# MAGIC CDR fixtures regenerate with identical content -- Auto Loader itself, not this notebook,
# MAGIC tracks which files it has already ingested via its checkpoint.
