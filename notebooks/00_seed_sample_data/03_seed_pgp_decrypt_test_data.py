# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-ING-005 Test Data: Landing PGP Decryption via UC Secrets
# MAGIC
# MAGIC Builds `flowx_testing/014_ing_005_pgp_decrypt.json`'s one and only fixture: a
# MAGIC financial transactions CSV, ZIPped, then PGP-encrypted for a dedicated, disposable,
# MAGIC test-only PGP keypair (`sample_data/flowx_testing/finance_usecase/
# MAGIC pgp_test_keypair_{public,private}.asc` -- zero real security value; generated purely for
# MAGIC this scenario). The archive is built and PGP-encrypted **directly on-cluster**, the same
# MAGIC way `02_seed_flowx_testing_data.py` builds its own ZIP fixtures in-process, rather than
# MAGIC syncing a pre-built binary through the bundle (workspace-bundle sync can mangle a
# MAGIC pre-built `.zip`/binary upload -- see `docs/05_deployment_guide.md`'s ASN.1 section).
# MAGIC
# MAGIC Reuses the framework's own `crypto/pgp.py::pgp_encrypt` -- the exact function whose
# MAGIC counterpart, `pgp_decrypt`, `ingestion/readers.py::_apply_source_zip_handling` calls at
# MAGIC pipeline runtime -- so this seed step genuinely exercises the same code path in reverse,
# MAGIC not a hand-rolled reimplementation.
# MAGIC
# MAGIC Before running the job this belongs to (`flowx_test_ing_005_pgp_decrypt_job`), the
# MAGIC private key half of this same keypair must already be provisioned as the UC secret
# MAGIC `{{catalog}}.security.pgp_private_key_finance_test` -- see `docs/35_tc_ing_005.md` for the
# MAGIC exact `databricks secrets put-secret` command.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module Bootstrap
# MAGIC
# MAGIC Only needed here to reuse `crypto/pgp.py::pgp_encrypt` -- see
# MAGIC `02_onboarding/02_onboarding_engine.py`'s identical cell for why the fallback exists.

# COMMAND ----------

import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_pgp_decrypt_test_data")

try:
    import flowx.lakeflow_framework  # noqa: F401
except ImportError:
    try:
        this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
        dev_src_root = os.path.abspath(os.path.join(this_dir, "..", "..", "src"))
        if dev_src_root not in sys.path:
            sys.path.insert(0, dev_src_root)
        import flowx.lakeflow_framework  # noqa: F401
        logger.warning("Loaded 'flowx' from local 'src/' (dev fallback) -- not from an installed wheel.")
    except ImportError as exc:
        raise ImportError(
            "Could not import 'flowx'. In production this must be attached as a "
            "wheel library (see resources/*.yml); for local development, run from within the repo so "
            f"'../../src' resolves. Original error: {exc}"
        ) from exc

from flowx.lakeflow_framework.crypto.pgp import pgp_encrypt  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets & Path Resolution

# COMMAND ----------

dbutils.widgets.text("catalog", "flowx", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FINANCE_FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "flowx_testing", "finance_usecase")

if not os.path.isdir(FINANCE_FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FINANCE_FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved finance_usecase fixture directory: %s", FINANCE_FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schema & Volumes

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.finance")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze_finance")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.finance.landing_pgp")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.finance._schemas")

logger.info("Provisioned catalog '%s' with the finance/bronze_finance schemas and landing_pgp/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build the ZIP In-Memory, Then PGP-Encrypt It
# MAGIC
# MAGIC Decrypt-then-unzip order at pipeline runtime is fixed (see
# MAGIC `docs/22_ingestion_pgp_zip_json_standardization.md` §1.1), so this seed step must build
# MAGIC the archive in the mirror-image order: ZIP first, then PGP-encrypt the whole ZIP's bytes.

# COMMAND ----------

import io
import zipfile

_txns_csv_path = os.path.join(FINANCE_FIXTURE_DIR, "txns_raw.csv")
_public_key_path = os.path.join(FINANCE_FIXTURE_DIR, "pgp_test_keypair_public.asc")

for _required_path in (_txns_csv_path, _public_key_path):
    if not os.path.exists(_required_path):
        raise FileNotFoundError(f"Required PGP decrypt test fixture missing: '{_required_path}'")

with open(_txns_csv_path, "r", encoding="utf-8", newline="") as _csv_file:
    _csv_text = _csv_file.read()

with open(_public_key_path, "r", encoding="utf-8") as _pubkey_file:
    _public_key_armored = _pubkey_file.read()

_zip_buffer = io.BytesIO()
with zipfile.ZipFile(_zip_buffer, "w", compression=zipfile.ZIP_DEFLATED) as _archive:
    _archive.writestr("txns_raw.csv", _csv_text)
_zip_bytes = _zip_buffer.getvalue()

_pgp_encrypted_bytes = pgp_encrypt(_zip_bytes, _public_key_armored)

logger.info(
    "Built and PGP-encrypted the financial_txns_202608 ZIP in-memory (%d plaintext ZIP bytes -> %d PGP-armored bytes).",
    len(_zip_bytes),
    len(_pgp_encrypted_bytes),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Land the PGP+ZIP Archive
# MAGIC
# MAGIC Filename matches `014_ing_005_pgp_decrypt.json`'s own
# MAGIC `source_config.source_zip_handling.zip_file_pattern` exactly.

# COMMAND ----------

_incoming_dir = f"/Volumes/{CATALOG}/finance/landing_pgp/incoming"
_target_path = f"{_incoming_dir}/financial_txns_202608.zip.pgp"

dbutils.fs.mkdirs(_incoming_dir)
with open(_target_path, "wb") as _destination:
    _destination.write(_pgp_encrypted_bytes)

logger.info("Landed PGP+ZIP fixture at '%s' (%d bytes).", _target_path, len(_pgp_encrypted_bytes))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `flowx_testing/014_ing_005_pgp_decrypt.json` can now be onboarded and its pipeline run
# MAGIC -- provided the matching private key has already been provisioned as the UC secret
# MAGIC `{{catalog}}.security.pgp_private_key_finance_test` (see `docs/35_tc_ing_005.md`). Re-running
# MAGIC this notebook is safe: the PGP+ZIP archive regenerates idempotently (same plaintext
# MAGIC content, overwritten each time).
