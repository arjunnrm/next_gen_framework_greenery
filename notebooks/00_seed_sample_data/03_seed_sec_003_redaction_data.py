# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-SEC-003 -- Secret Masking & Redaction in Telemetry Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `metaflow_testing/035_sec_003_redaction.json` only --
# MAGIC intentionally separate from every other scenario's own seed notebook, so this test case's
# MAGIC build/run stays isolated.
# MAGIC
# MAGIC Provisions:
# MAGIC * `sec` schema + `landing_audit`/`_schemas` Volumes (landing zone for the ingestion flow's
# MAGIC   Auto Loader source) -- `bronze_sec` (the DLT-managed target schema for this scenario's one
# MAGIC   `target_type: "external_sink"` flow) is created automatically by the pipeline's own
# MAGIC   `CREATE SCHEMA IF NOT EXISTS` at deployment time, same as every other scenario, so this
# MAGIC   notebook does not touch it.
# MAGIC * `egress_sec` schema + `audit_zips`/`audit_zips_export` Volumes -- the staging and final
# MAGIC   destination Volumes this flow's `target_config.sink_config` (format `pgp_zip`) writes into.
# MAGIC   A sink's destination Volume is never auto-created by the pipeline itself (see
# MAGIC   `docs/63_tc_snk_003.md`, which established this exact same provisioning need for
# MAGIC   TC-SNK-003's own `finance_egress`/`secure_drops` Volumes) -- this is the same kind of new
# MAGIC   provisioning step, under a dedicated `egress_sec` schema so this scenario's own egress
# MAGIC   archives never mix with TC-SNK-003's.
# MAGIC
# MAGIC Then lands a small, 5-row credential-vault access-audit fixture
# MAGIC (`secrets_audit_batch1.csv` -- plaintext `secret_value` values, as they would arrive from a
# MAGIC real vault's own audit export) in the incoming Volume. This scenario's own ingestion flow
# MAGIC (`df_sec_003_secrets_audit_ingest`) is what both encrypts `secret_value` via
# MAGIC `target_config.encrypted_columns` AND PGP-exports the resulting row set via
# MAGIC `target_config.sink_config` before/as the target table is materialized -- this notebook only
# MAGIC lands the raw, still-plaintext source file; it does not touch encryption or the PGP egress
# MAGIC sink at all. See `docs/56_tc_sec_003.md` for the full scenario writeup, INCLUDING why this
# MAGIC test case is expected to currently surface (not silently pass around) a known, unresolved
# MAGIC secret-redaction gap in `crypto/column_crypto.py`.
# MAGIC
# MAGIC Built from `sample_data/metaflow_testing/sec_usecase/secrets_audit_batch1.csv`.
# MAGIC
# MAGIC **Does not provision either Unity Catalog secret this scenario depends on** -- both are
# MAGIC reused verbatim from earlier scenarios, not newly provisioned here:
# MAGIC * `{{catalog}}.security.pii_encryption_key` -- see `docs/05_deployment_guide.md` Sec.0
# MAGIC   prerequisites (every encryption scenario in this project already depends on this one).
# MAGIC * `{{catalog}}.security.pgp_public_key_finance_egress_test` -- provisioned by
# MAGIC   `metaflow_test_snk_003_pgp_zip_sink_job` (see `docs/63_tc_snk_003.md`) from the disposable
# MAGIC   test keypair at `sample_data/metaflow_testing/finance_egress_usecase/
# MAGIC   pgp_egress_keypair_public.asc`. See `docs/56_tc_sec_003.md` for the fallback command if
# MAGIC   TC-SNK-003 hasn't been run against this workspace yet.
# MAGIC
# MAGIC Both secrets must already exist in the target workspace/catalog before this job's
# MAGIC `run_pipeline` task runs, exactly as for every other encryption/PGP scenario in this project.
# MAGIC
# MAGIC Run once per environment before onboarding `metaflow_testing/035_sec_003_redaction.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_sec_003_redaction_data")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "metaflow_testing", "sec_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved sec_usecase fixture directory: %s", FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Landing Schema & Volumes
# MAGIC
# MAGIC `bronze_sec` (this flow's materialized target schema) is created automatically by the
# MAGIC pipeline's own `CREATE SCHEMA IF NOT EXISTS` at deployment time -- no need to pre-create it
# MAGIC here.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.sec")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.sec.landing_audit")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.sec._schemas")

logger.info("Provisioned catalog '%s' with sec schema + landing_audit/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Provision Egress Schema & Volumes (PGP-ZIP Sink)
# MAGIC
# MAGIC Not pipeline-managed -- `sink_config.path`/`post_export_archive.output_zip_path` write
# MAGIC directly to these Volumes' underlying storage, so they must already exist as real Unity
# MAGIC Catalog Volume objects before `run_pipeline` executes (same requirement `docs/63_tc_snk_003.md`
# MAGIC documents for TC-SNK-003's own egress Volumes).

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.egress_sec")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.egress_sec.audit_zips")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.egress_sec.audit_zips_export")

dbutils.fs.mkdirs(f"/Volumes/{CATALOG}/egress_sec/audit_zips/_staging")

logger.info(
    "Provisioned catalog '%s' with egress_sec schema + audit_zips/audit_zips_export volumes.",
    CATALOG,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Land the Secrets-Audit Fixture

# COMMAND ----------

AUDIT_INCOMING_ZONE = f"/Volumes/{CATALOG}/sec/landing_audit/incoming"
dbutils.fs.mkdirs(AUDIT_INCOMING_ZONE)

_audit_fixture = os.path.join(FIXTURE_DIR, "secrets_audit_batch1.csv")
if not os.path.exists(_audit_fixture):
    raise FileNotFoundError(f"Required secrets-audit fixture missing: '{_audit_fixture}'")

dbutils.fs.cp(f"file:{_audit_fixture}", f"{AUDIT_INCOMING_ZONE}/secrets_audit_batch1.csv")
logger.info(
    "Landed secrets-audit fixture at '%s/secrets_audit_batch1.csv' (5 rows, plaintext secret_value).",
    AUDIT_INCOMING_ZONE,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `metaflow_testing/035_sec_003_redaction.json` can now be onboarded and its pipeline run --
# MAGIC `{{catalog}}.bronze_sec.secrets_audit_raw` should end up with 5 rows, `secret_value` stored
# MAGIC as AES-GCM ciphertext, plus a `.zip.pgp` archive under
# MAGIC `/Volumes/{catalog}/egress_sec/audit_zips_export/` containing that SAME ciphertext row set
# MAGIC (never the plaintext `secret_value` values landed here). See `docs/56_tc_sec_003.md` for the
# MAGIC full verification queries -- AND for why this test case's real assertion (no plaintext key
# MAGIC ever appears in a Spark query plan/driver log) is expected to currently FAIL, not pass.
# MAGIC
# MAGIC Re-running this notebook is safe and idempotent: it only ever re-copies the same fixture
# MAGIC file (Auto Loader tracks already-ingested files via its own checkpoint, so re-landing the
# MAGIC same filename does not re-ingest or duplicate rows).
