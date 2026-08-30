# Databricks notebook source
# MAGIC %md
# MAGIC # Provision Governance UDFs and Entitlements (Crypto/ABAC Test Suite)
# MAGIC
# MAGIC **v2 schema note:** governance moved to a tags-only model (`governance/tags.py`) --
# MAGIC `apply_governance_tags` applies descriptive key-value tags via
# MAGIC `ALTER TABLE ... SET TAGS`, but no longer **binds** a UC row-filter/column-mask
# MAGIC function to anything (that's a workspace admin's tag-policy configuration, external
# MAGIC to this repo). The row-filter/column-mask functions and entitlements table this
# MAGIC notebook provisions below are kept as a **reference example** of what a real,
# MAGIC attribute-based, `current_user()`-driven enforcement function looks like -- useful
# MAGIC if a workspace admin wants to bind a UC tag policy that calls them -- but the
# MAGIC framework itself no longer wires them to `spec_20`'s `governance_tags` automatically
# MAGIC -- see docs/20_crypto_abac_exhaustive_test_suite.md for the tag-verification approach.
# MAGIC
# MAGIC Idempotent: every statement is `CREATE ... IF NOT EXISTS` / `CREATE OR REPLACE
# MAGIC FUNCTION` / a keyed `MERGE`, so re-running this notebook (e.g. as part of a job
# MAGIC re-run) never fails on "already exists" and never duplicates the entitlement row.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("provision_governance_udfs_and_entitlements")

dbutils.widgets.text("catalog", "poc", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Governance Schema + Entitlements Table
# MAGIC
# MAGIC `user_entitlements` is the single source of truth both UDFs below consult. Since
# MAGIC this workspace only has one principal available to run live queries as, "two
# MAGIC roles" for the row-filter/column-mask enforcement proofs are simulated by mutating
# MAGIC *this one principal's own row* between two live query passes -- a genuine,
# MAGIC per-query, live re-evaluation of the ABAC predicate, just without a second real
# MAGIC Unity Catalog identity. See `docs/20_crypto_abac_exhaustive_test_suite.md`'s design
# MAGIC notes for why this is still a real enforcement proof, and what it does *not* prove
# MAGIC (Unity Catalog's own cross-principal privilege isolation).

# COMMAND ----------

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.governance")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.governance.user_entitlements (
          user_email STRING,
          allowed_region STRING,
          can_view_pii BOOLEAN
        ) USING DELTA
        COMMENT 'Crypto/ABAC exhaustive test suite: per-principal entitlements consulted live by fn_crypto_abac_region_filter / fn_crypto_abac_mask_ssn.'
        """
    )
    spark.sql(
        f"""
        MERGE INTO {CATALOG}.governance.user_entitlements t
        USING (SELECT current_user() AS user_email, 'US-EAST' AS allowed_region, false AS can_view_pii) s
        ON t.user_email = s.user_email
        WHEN MATCHED THEN UPDATE SET t.allowed_region = s.allowed_region, t.can_view_pii = s.can_view_pii
        WHEN NOT MATCHED THEN INSERT (user_email, allowed_region, can_view_pii) VALUES (s.user_email, s.allowed_region, s.can_view_pii)
        """
    )
    logger.info("Provisioned '%s.governance.user_entitlements' with a seeded row for the current principal.", CATALOG)
except Exception as exc:  # noqa: BLE001
    raise RuntimeError(f"Failed to provision governance entitlements table for '{CATALOG}': {exc}") from exc

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Row Filter Function: `fn_crypto_abac_region_filter`
# MAGIC
# MAGIC Genuinely attribute-based, not a static predicate -- looks up the *querying*
# MAGIC principal's `allowed_region` live, per query, rather than hardcoding a region.
# MAGIC Reference: [Row filters](https://docs.databricks.com/en/tables/row-and-column-filters.html).

# COMMAND ----------

try:
    spark.sql(
        f"""
        CREATE OR REPLACE FUNCTION {CATALOG}.governance.fn_crypto_abac_region_filter(region_code STRING)
        RETURNS BOOLEAN
        COMMENT 'Crypto/ABAC exhaustive test suite: allows a row only if the querying principal is entitled to its region_code.'
        RETURN EXISTS (
          SELECT 1 FROM {CATALOG}.governance.user_entitlements e
          WHERE e.user_email = current_user() AND e.allowed_region = region_code
        )
        """
    )
    logger.info("Provisioned row filter function '%s.governance.fn_crypto_abac_region_filter'.", CATALOG)
except Exception as exc:  # noqa: BLE001
    raise RuntimeError(f"Failed to provision fn_crypto_abac_region_filter for '{CATALOG}': {exc}") from exc

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2b. A Second Row Filter Function: `fn_crypto_abac_region_filter_v2`
# MAGIC
# MAGIC Identical logic to `fn_crypto_abac_region_filter` above -- exists solely so the
# MAGIC verification notebook's "changed policy forces exactly one reapply" scenario has a
# MAGIC genuinely different, but still DDL-valid (same single-`STRING`-parameter signature),
# MAGIC `function_name` to switch to. Changing `using_columns`' arity instead would make
# MAGIC `ALTER TABLE ... SET ROW FILTER` itself fail (the function only accepts one
# MAGIC parameter), which would test a DDL error, not a genuine config-change/reapply path.

# COMMAND ----------

try:
    spark.sql(
        f"""
        CREATE OR REPLACE FUNCTION {CATALOG}.governance.fn_crypto_abac_region_filter_v2(region_code STRING)
        RETURNS BOOLEAN
        COMMENT 'Crypto/ABAC exhaustive test suite: identical to fn_crypto_abac_region_filter, used only to prove a changed function_name forces exactly one policy reapply.'
        RETURN EXISTS (
          SELECT 1 FROM {CATALOG}.governance.user_entitlements e
          WHERE e.user_email = current_user() AND e.allowed_region = region_code
        )
        """
    )
    logger.info("Provisioned row filter function '%s.governance.fn_crypto_abac_region_filter_v2'.", CATALOG)
except Exception as exc:  # noqa: BLE001
    raise RuntimeError(f"Failed to provision fn_crypto_abac_region_filter_v2 for '{CATALOG}': {exc}") from exc

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Column Mask Function: `fn_crypto_abac_mask_ssn`
# MAGIC
# MAGIC Partial redaction (last 4 digits visible) unless the querying principal's
# MAGIC `can_view_pii` entitlement is true. Reference:
# MAGIC [Column masks](https://docs.databricks.com/en/tables/row-and-column-filters.html).

# COMMAND ----------

try:
    spark.sql(
        f"""
        CREATE OR REPLACE FUNCTION {CATALOG}.governance.fn_crypto_abac_mask_ssn(ssn_plain STRING)
        RETURNS STRING
        COMMENT 'Crypto/ABAC exhaustive test suite: partially redacts ssn_plain unless the querying principal has can_view_pii entitlement.'
        RETURN CASE
          WHEN EXISTS (SELECT 1 FROM {CATALOG}.governance.user_entitlements e WHERE e.user_email = current_user() AND e.can_view_pii)
          THEN ssn_plain
          ELSE concat('***-**-', right(ssn_plain, 4))
        END
        """
    )
    logger.info("Provisioned column mask function '%s.governance.fn_crypto_abac_mask_ssn'.", CATALOG)
except Exception as exc:  # noqa: BLE001
    raise RuntimeError(f"Failed to provision fn_crypto_abac_mask_ssn for '{CATALOG}': {exc}") from exc

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Negative-Path Fixture: a Row-Filter Function That Deliberately Does *Not* Exist
# MAGIC
# MAGIC No provisioning needed here -- `abac_negative_missing_function` (see the
# MAGIC verification notebook) deliberately references
# MAGIC `{{catalog}}.governance.fn_does_not_exist`, proving `apply_uc_abac_policies` catches
# MAGIC and reports a real binding failure rather than assuming the function was created.

# COMMAND ----------

logger.info("Governance UDF/entitlement provisioning complete for catalog '%s'.", CATALOG)
