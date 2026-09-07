-- =====================================================================================
-- 03_grants.sql
-- Unity Catalog grants for the UC3 / UC6 / UC7 consumer groups.
--
-- CATALOG
--   Every statement targets `br_digital_poc`.
--
-- PREREQUISITE
--   The account groups named here must already exist and be assigned to the workspace.
--   A GRANT to a non-existent group fails.
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- 1. UC7 — call detail records (RESTRICTED)
--
-- Granted at TABLE granularity. See the two prohibitions in section 4 for why this is
-- not done at schema level.
-- -------------------------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG br_digital_poc              TO `uc7_restricted_readers`;
GRANT USE SCHEMA  ON SCHEMA  br_digital_poc.bronze       TO `uc7_restricted_readers`;

GRANT SELECT ON TABLE br_digital_poc.bronze.emsc_cdr_raw TO `uc7_restricted_readers`;
GRANT SELECT ON TABLE br_digital_poc.bronze.psgw_cdr_raw TO `uc7_restricted_readers`;
GRANT SELECT ON TABLE br_digital_poc.bronze.sgsn_cdr_raw TO `uc7_restricted_readers`;
GRANT SELECT ON TABLE br_digital_poc.bronze.tap310_raw   TO `uc7_restricted_readers`;


-- -------------------------------------------------------------------------------------
-- 2. UC6 — flood warning consumers read the CONFORMED layers, not Bronze
-- -------------------------------------------------------------------------------------
GRANT USE SCHEMA ON SCHEMA br_digital_poc.gold                        TO `flood_warning_analysts`;
GRANT SELECT ON TABLE br_digital_poc.gold.flood_warning_summary       TO `flood_warning_analysts`;


-- -------------------------------------------------------------------------------------
-- 3. Engineering — landing volume for UC7 operations
-- -------------------------------------------------------------------------------------
GRANT READ VOLUME ON VOLUME br_digital_poc.landing.uc_7 TO `flowx_engineering`;


-- =====================================================================================
-- 4. TWO GRANTS THAT MUST NOT BE MADE
--
-- Both are easy mistakes with real consequences. They are recorded here, as comments,
-- because the place a mistaken grant gets typed is a grants script.
--
--
-- (a) NEVER `GRANT SELECT ON SCHEMA br_digital_poc.bronze` to any consumer group.
--
--     That schema mixes all three use cases. A grant intended to give a team its own
--     Bronze tables also hands it UC7's call detail records — MSISDN, IMSI, IMEI and
--     cell-site location. In this schema, grant at TABLE granularity, always.
--
--
-- (b) NEVER grant `READ VOLUME` on `uc_6` broadly.
--
--     The `output/` subtree holds egress files destined for external parties, and volume
--     grants are NOT path-scoped — there is no way to grant `raw/` without also granting
--     `output/`. A grant meant to let someone inspect inputs also hands them the
--     outbound files.
-- =====================================================================================
