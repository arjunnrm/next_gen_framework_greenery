-- =====================================================================================
-- 02_column_masks_and_row_filters.sql
--
-- STATUS: NOT APPLIED. This script is a proposed implementation, not a record of the
-- estate. No column mask and no row filter exists anywhere in the deployment today.
-- Tags are applied but never ENFORCED: classification is descriptive metadata only, and
-- nothing currently restricts access to a RESTRICTED column.
--
-- Do not run this script until enforcement has been agreed and the reader groups named
-- below actually exist. Running it against groups that do not exist will fail; running it
-- against the wrong groups will silently deny or expose data.
--
-- CATALOG
--   Every statement targets `br_digital_poc`.
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- SEQUENCING — two rules that are not optional
--
-- 1. Masks and row filters are Unity Catalog DDL and must run AFTER the pipeline update,
--    exactly as tag DDL does. Applying them before the table exists fails, and applying
--    them from inside a pipeline is not possible.
--
-- 2. Take care masking a Bronze column that a Silver flow depends on. A mask applies to
--    the pipeline's own reads as well as to a human's, so masking a Bronze JOIN KEY can
--    silently change what the downstream transformation computes — the pipeline will not
--    error, it will simply produce different answers. Mask at the CONSUMPTION layer in
--    preference to Bronze.
-- -------------------------------------------------------------------------------------


-- -------------------------------------------------------------------------------------
-- 1. Column mask — subscriber identifier
--
-- Full value for the entitled group; last four digits for everyone else.
-- -------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION br_digital_poc.config.mask_msisdn(value STRING)
RETURN CASE
         WHEN is_account_group_member('uc7_restricted_readers') THEN value
         ELSE CONCAT('*******', RIGHT(value, 4))
       END;

ALTER TABLE br_digital_poc.bronze.sgsn_cdr_raw
  ALTER COLUMN msisdn SET MASK br_digital_poc.config.mask_msisdn;


-- -------------------------------------------------------------------------------------
-- 2. Row filter — restrict a Gold view to the caller's own region
-- -------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION br_digital_poc.config.filter_region(region STRING)
RETURN is_account_group_member('flood_warning_all_regions')
       OR region = current_user_region();

ALTER TABLE br_digital_poc.gold.flood_warning_summary
  SET ROW FILTER br_digital_poc.config.filter_region ON (region);
