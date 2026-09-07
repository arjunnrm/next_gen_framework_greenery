-- =====================================================================================
-- 01_container_tags.sql
-- Container-level Unity Catalog tag DDL for UC3, UC6 and UC7.
--
-- WHY THIS SCRIPT EXISTS
--   The framework's governance engine tags TABLES and COLUMNS only. Catalog, schema and
--   volume tags have no route through an onboarding spec and must be applied by hand.
--   This script is that hand-application, kept in one place so the tag vocabulary cannot
--   drift between documents and the estate.
--
-- WHEN TO RUN
--   After `bundle deploy` and after the first pipeline update has created the tables.
--   Tag DDL applied to a table that does not yet exist fails; it is never run inside a
--   pipeline.
--
-- CATALOG
--   Every statement targets `br_digital_poc`. If you are deploying into a different
--   catalog, change the name in one place: the :catalog parameter marker below, or a
--   find-and-replace of `br_digital_poc` across this file.
--
-- PRIVILEGE
--   `ASSIGN` is required on each securable being tagged.
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- 1. Catalog
-- -------------------------------------------------------------------------------------
ALTER CATALOG br_digital_poc SET TAGS (
  'environment' = 'poc',
  'owner'       = 'business_data_and_ai'
);


-- -------------------------------------------------------------------------------------
-- 2. Schemas
--
-- NOTE: tags do NOT propagate. A schema tag does not reach the tables inside it, so the
-- table-level tags in section 4 are additional, not redundant.
-- -------------------------------------------------------------------------------------
ALTER SCHEMA br_digital_poc.bronze  SET TAGS ('environment' = 'poc', 'data_classification' = 'confidential');
ALTER SCHEMA br_digital_poc.landing SET TAGS ('environment' = 'poc', 'data_classification' = 'restricted');
ALTER SCHEMA br_digital_poc.silver  SET TAGS ('environment' = 'poc', 'data_classification' = 'confidential');
ALTER SCHEMA br_digital_poc.gold    SET TAGS ('environment' = 'poc', 'data_classification' = 'confidential');


-- -------------------------------------------------------------------------------------
-- 3. Volumes
-- -------------------------------------------------------------------------------------
ALTER VOLUME br_digital_poc.landing.uc_7 SET TAGS ('use_case' = 'uc7', 'data_classification' = 'restricted');
ALTER VOLUME br_digital_poc.staging.uc_6 SET TAGS ('use_case' = 'uc6', 'data_classification' = 'confidential');
ALTER VOLUME br_digital_poc.staging.uc_3 SET TAGS ('use_case' = 'uc3', 'data_classification' = 'confidential');


-- -------------------------------------------------------------------------------------
-- 4. Tables and columns — REFERENCE SHAPE ONLY
--
-- Prefer the `governance_tags` block in the onboarding spec for anything at table or
-- column level. The engine emits exactly the statements below, and a tag set through the
-- spec is reproducible on every run; a tag set here is not.
--
-- These two statements are retained so the emitted shape is documented, and for the one
-- case the spec route cannot cover: re-tagging a table after a manual intervention.
-- -------------------------------------------------------------------------------------
ALTER TABLE br_digital_poc.bronze.sgsn_cdr_raw SET TAGS ('use_case' = 'uc7', 'pii' = 'true');
ALTER TABLE br_digital_poc.bronze.sgsn_cdr_raw ALTER COLUMN msisdn SET TAGS ('pii' = 'true');


-- =====================================================================================
-- VERIFICATION
--
-- There is NO `SHOW TAGS` statement in Unity Catalog. `information_schema` is the only
-- way to read tags back — and it must ALWAYS be catalog-qualified.
--
-- An unqualified `information_schema` query resolves against `current_catalog()`.
-- Running it while positioned in a different catalog returns ZERO ROWS, truthfully and
-- without error, which reads exactly like "no tags are applied" when the tags exist and
-- the query is simply looking in the wrong place. This caused a real and long-lived
-- misdiagnosis on this project.
-- =====================================================================================

-- WRONG — resolves against current_catalog(), whatever that happens to be:
--   SELECT * FROM information_schema.table_tags WHERE schema_name = 'bronze';

-- RIGHT — always qualify with the catalog:
SELECT * FROM br_digital_poc.information_schema.table_tags  WHERE schema_name = 'bronze';
SELECT * FROM br_digital_poc.information_schema.column_tags WHERE schema_name = 'bronze';
SELECT * FROM br_digital_poc.information_schema.catalog_tags;
SELECT * FROM br_digital_poc.information_schema.schema_tags;
SELECT * FROM br_digital_poc.information_schema.volume_tags;
