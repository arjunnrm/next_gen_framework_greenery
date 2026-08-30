# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-SNK-003 -- PGP Encrypted ZIP Sink Export Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `metaflow_testing/041_snk_003_pgp_zip_sink.json` only --
# MAGIC intentionally separate from `02_seed_metaflow_testing_data.py` and
# MAGIC `03_seed_pgp_decrypt_test_data.py` so this test case's build/run stays isolated from those
# MAGIC other scenarios' own fixtures.
# MAGIC
# MAGIC Lands a small plaintext `monthly_settlements.csv` (5 rows) directly in the incoming
# MAGIC landing Volume -- **no ZIP/PGP handling on the INPUT side**, that's deliberate: this
# MAGIC scenario's whole point is the framework's genuine Lakeflow sink doing PGP+ZIP work on
# MAGIC **egress** (`target_type: "external_sink"`, `sink_config.format: "pgp_zip"`,
# MAGIC `pgp_encryption.enabled: true` -- see `engine/sink_registration.py::_build_sink_options`
# MAGIC and `archive/pgp_zip_sink.py`), not on ingest (unlike TC-ING-005's
# MAGIC `03_seed_pgp_decrypt_test_data.py`, which encrypts an INPUT fixture for the pipeline to
# MAGIC decrypt). This notebook therefore never touches the PGP keypair at all -- it only lands the
# MAGIC plaintext CSV.
# MAGIC
# MAGIC Also provisions the `finance_egress`/`secure_drops` schema+Volume the sink writes its
# MAGIC finished `.zip.pgp` archives into, mirroring `02_seed_metaflow_testing_data.py`'s own
# MAGIC `egress_ea`/`export_zips` provisioning for TC-SNK-001's plain-ZIP sink -- a sink's
# MAGIC destination Volume is never auto-created by the pipeline itself (only the pipeline's own
# MAGIC default `catalog`/`schema` -- here `metaflow.silver_finance` -- gets that treatment).
# MAGIC
# MAGIC Before `metaflow_test_snk_003_pgp_zip_sink_job`'s `run_pipeline_task` can succeed, the
# MAGIC recipient's PUBLIC key half of a dedicated, disposable, test-only PGP keypair
# MAGIC (`sample_data/metaflow_testing/finance_egress_usecase/
# MAGIC pgp_egress_keypair_{public,private}.asc`) must already be provisioned as the UC secret
# MAGIC `{{catalog}}.security.pgp_public_key_finance_egress_test` -- see `docs/63_tc_snk_003.md` for
# MAGIC the exact `databricks secrets put-secret` command.
# MAGIC
# MAGIC Run once per environment before onboarding `metaflow_testing/041_snk_003_pgp_zip_sink.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_snk_003_pgp_zip_sink_data")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "metaflow_testing", "finance_egress_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved finance_egress_usecase fixture directory: %s", FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes
# MAGIC
# MAGIC `silver_finance` (the pipeline's own default catalog/schema, where
# MAGIC `monthly_settlements` is actually materialized) is created automatically by the pipeline's
# MAGIC own `CREATE SCHEMA IF NOT EXISTS` at deployment time, same as every other scenario in this
# MAGIC repo -- not seeded here. `finance`/`landing_settlements` (the INPUT landing zone) and
# MAGIC `finance_egress`/`secure_drops` (the sink's OUTPUT Volume) are not the pipeline's own
# MAGIC default schema, so both need explicit provisioning below.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.finance")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.finance.landing_settlements")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.finance._schemas")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.finance_egress")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.finance_egress.secure_drops")

logger.info(
    "Provisioned catalog '%s' with finance/landing_settlements+_schemas volumes and the "
    "finance_egress/secure_drops sink output volume.",
    CATALOG,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the Plaintext Settlements CSV

# COMMAND ----------

SETTLEMENTS_INCOMING_ZONE = f"/Volumes/{CATALOG}/finance/landing_settlements/incoming"
dbutils.fs.mkdirs(SETTLEMENTS_INCOMING_ZONE)

_fixture_path = os.path.join(FIXTURE_DIR, "monthly_settlements.csv")
if not os.path.exists(_fixture_path):
    raise FileNotFoundError(f"Required settlements fixture missing: '{_fixture_path}'")

dbutils.fs.cp(f"file:{_fixture_path}", f"{SETTLEMENTS_INCOMING_ZONE}/monthly_settlements.csv")
logger.info("Landed settlements fixture at '%s/monthly_settlements.csv'", SETTLEMENTS_INCOMING_ZONE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `metaflow_testing/041_snk_003_pgp_zip_sink.json` can now be onboarded and its pipeline run
# MAGIC -- provided the recipient public key has already been provisioned as the UC secret
# MAGIC `{{catalog}}.security.pgp_public_key_finance_egress_test` (see `docs/63_tc_snk_003.md`).
# MAGIC Re-running this notebook is safe: the CSV fixture is copied fresh each time (Auto Loader
# MAGIC itself, not this notebook, tracks which files it has already ingested via its checkpoint,
# MAGIC so re-landing the same filename does not re-ingest or duplicate rows).
