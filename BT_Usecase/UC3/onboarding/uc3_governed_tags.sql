-- =====================================================================
-- UC3 (Excalibur / CRM) — Unity Catalog GOVERNED TAG definitions
--
-- Derived from the governance_tags blocks in:
--   BT_Usecase/UC3/onboarding/uc3_excalibur_streaming_cdc.json
--   BT_Usecase/UC3/onboarding/uc3_excalibur_batch_recon.json
--
-- 14 distinct tag keys: 5 table-level, 9 column-level (no overlap).
--
-- Run as METASTORE ADMIN, ONCE PER METASTORE, BEFORE the
-- `apply_governance_tags` job task runs. Tag policies are metastore-wide
-- (not catalog-scoped), so this file is target-independent.
--
-- The framework applies these tags via ALTER TABLE ... SET TAGS in
-- src/flowx/lakeflow_framework/governance/tags.py. That module does NOT
-- validate tag VALUES, and spec_validator.py does not either — the
-- allowed-value lists below are the ONLY thing that constrains them.
-- Once a governed tag exists, SET TAGS with an out-of-list value FAILS.
-- =====================================================================


-- ---------------------------------------------------------------------
-- SECTION 1 — TABLE-LEVEL TAGS (5)
-- Applied to: <catalog>.bronze.{customer,subscriber,physical_device}
--             <catalog>.staging.{customer,subscriber,physical_device}_batch
-- ---------------------------------------------------------------------

CREATE TAG POLICY IF NOT EXISTS source_system
  ALLOWED_VALUES ('excalibur')
  COMMENT 'Originating source system of the dataset.';

CREATE TAG POLICY IF NOT EXISTS use_case
  ALLOWED_VALUES ('uc3')
  COMMENT 'BT use-case identifier that owns the dataset.';

CREATE TAG POLICY IF NOT EXISTS domain
  ALLOWED_VALUES ('Customer 360')
  COMMENT 'Business data domain.';
-- The UC3 specs moved from domain = 'crm' to the governed value 'Customer 360' on
-- 2026-09-08. If the policy was created earlier with ('crm'), widen it in place instead
-- of re-creating it (CREATE ... IF NOT EXISTS is a no-op on an existing policy):
--   ALTER TAG POLICY domain ADD ALLOWED_VALUES ('Customer 360');
-- Governed-tag values are case- and space-sensitive; the framework single-quotes the
-- value verbatim: ALTER TABLE <catalog>.bronze.customer SET TAGS ('domain' = 'Customer 360').

CREATE TAG POLICY IF NOT EXISTS layer
  ALLOWED_VALUES ('staging')
  COMMENT 'Medallion layer of the target table.';

CREATE TAG POLICY IF NOT EXISTS load_pattern
  ALLOWED_VALUES ('batch')
  COMMENT 'Ingestion pattern that populates the table.';


-- ---------------------------------------------------------------------
-- SECTION 2 — COLUMN-LEVEL TAGS (9)
-- ---------------------------------------------------------------------

-- 2.1 Sensitivity / tokenisation controls -----------------------------

CREATE TAG POLICY IF NOT EXISTS sensitive
  ALLOWED_VALUES ('Y', 'N')
  COMMENT 'Column holds sensitive data requiring restricted handling.';

CREATE TAG POLICY IF NOT EXISTS tokenise_pii
  ALLOWED_VALUES ('Y', 'N', '?')
  COMMENT 'Column must be tokenised as PII. "?" = classification undecided.';

CREATE TAG POLICY IF NOT EXISTS data_fabric_action
  ALLOWED_VALUES ('NULL_AT_SOURCE')
  COMMENT 'Remediation action Data Fabric applies to the column.';


-- 2.2 Downstream egress / read-only entitlements ----------------------

CREATE TAG POLICY IF NOT EXISTS csql_ro
  ALLOWED_VALUES ('Y', 'N')
  COMMENT 'Column exposed to the CloudSQL read-only replica.';

CREATE TAG POLICY IF NOT EXISTS csql_secured_ro
  ALLOWED_VALUES ('Y', 'N')
  COMMENT 'Column exposed to the CloudSQL secured read-only replica.';

CREATE TAG POLICY IF NOT EXISTS bq_deid_ro
  ALLOWED_VALUES ('Y', 'N', 'Y-Hash')
  COMMENT 'BigQuery de-identified read-only exposure. "Y-Hash" = hashed.';


-- 2.3 Classification taxonomy ----------------------------------------

CREATE TAG POLICY IF NOT EXISTS data_class
  ALLOWED_VALUES (
    'BT_ACCOUNT_NUMBER',
    'CUSTOMER_ID',
    'EIN_NUMBER',
    'EMAIL_ADDRESS',
    'MSISDN',
    'NAME',
    'PHONE_NUMBER'
  )
  COMMENT 'Machine-readable data class (UPPER_SNAKE) driving tokenisation policy.';

CREATE TAG POLICY IF NOT EXISTS info_type
  ALLOWED_VALUES (
    'BT Account Number',
    'Customer ID',
    'EIN Number',
    'Email address',
    'MSISDN',
    'Name',
    'Phone Number'
  )
  COMMENT 'Human-readable information type; display label paired with data_class.';
  -- NOTE: the streaming spec once carried BOTH 'EIN Number' and 'EIN number'
  -- (case-differing duplicates on the physical_device and subscriber operator_id
  -- columns). Fixed to 'EIN Number' on 2026-09-08; only that spelling is allowed here.

CREATE TAG POLICY IF NOT EXISTS pdbt
  ALLOWED_VALUES (
    'BT Account Number',
    'Company Registration Number (CRN)',
    'Customer ID - Individual Externally Identifiable',
    'Customer ID - Individual Internally Identifiable',
    'Employee ID (EIN)',
    'Individual Date of Birth',
    'Individual Email Address',
    'Individual Gender',
    'Individual Name',
    'Individual Nationality',
    'Individual Self Disclosed Disability',
    'Individual Telephone Number',
    'MSISDN',
    'Supplier ID',
    'UK Taxpayer Reference (UTR) Number'
  )
  COMMENT 'BT Personal Data Business Term (PDBT) classification.';


-- ---------------------------------------------------------------------
-- SECTION 3 — GRANT ASSIGN so the pipeline service principal can tag
-- Replace <service-principal> with the identity running
-- notebooks/04_governance/04_apply_governance_and_egress.py.
-- Without APPLY_TAG the SET TAGS DDL fails with PERMISSION_DENIED.
-- ---------------------------------------------------------------------

-- GRANT APPLY TAG ON CATALOG <catalog> TO `<service-principal>`;


-- ---------------------------------------------------------------------
-- SECTION 4 — VERIFICATION
-- information_schema is CATALOG-SCOPED: always qualify the catalog, or a
-- session defaulted to `workspace` truthfully returns 0 rows for the
-- wrong catalog. There is no SHOW TAGS statement.
-- ---------------------------------------------------------------------

-- Governed tags that now exist metastore-wide:
-- SELECT * FROM system.information_schema.tag_policies ORDER BY tag_name;

-- Table tags landed on UC3 targets:
-- SELECT * FROM <catalog>.information_schema.table_tags
--  WHERE schema_name IN ('bronze', 'staging') ORDER BY table_name, tag_name;

-- Column tags landed on UC3 targets:
-- SELECT * FROM <catalog>.information_schema.column_tags
--  WHERE schema_name IN ('bronze', 'staging') ORDER BY table_name, column_name, tag_name;
