-- =====================================================================================
-- UC7 CDR ASN.1 - Analysis, Join and Export Query Pack
-- =====================================================================================
--
-- PURPOSE
--   A self-contained, copy-paste query pack for anybody who wants to open UC7 in a
--   Databricks SQL editor and test it for themselves. Every query below was executed
--   against the live workspace before being written down. No query is theoretical.
--
-- HOW TO RUN
--   1. Open Databricks SQL Editor, or a notebook cell with %sql.
--   2. Attach any SQL warehouse. No cluster libraries are needed.
--   3. Run the whole file top to bottom, or copy one numbered query at a time.
--   4. Nothing here writes, updates or deletes. Every statement is read-only.
--
-- ENVIRONMENT
--   Catalog : flowx          Schema : bronze
--   Tables  : emsc_cdr_raw, psgw_cdr_raw, sgsn_cdr_raw, tap310_raw
--             plus one _quarantine sibling per table
--
-- READING THE RESULTS
--   Each query states its EXPECTED result. If your figures differ, the pipeline has
--   been re-run with different input files. That is normal. What must NOT change is
--   the SHAPE of the answer, for example zero quarantined rows and zero decode errors.
--
-- =====================================================================================

USE CATALOG flowx;
USE SCHEMA bronze;


-- =====================================================================================
-- PART A - INVENTORY. What exists, and how much of it.
-- =====================================================================================

-- A1. Every UC7 table, with its type.
--     EXPECT: 8 rows. Four business tables plus four quarantine siblings.
--             All eight are STREAMING_TABLE.
--     WHY   : Confirms the pipeline published what it was asked to publish, and that
--             the quarantine siblings exist even when they are empty.
SELECT table_schema,
       table_name,
       table_type
FROM   br_digital_poc.information_schema.tables
WHERE  table_schema = 'bronze'
  AND  (table_name LIKE '%cdr_raw%' OR table_name LIKE 'tap310%')
ORDER  BY table_name;


-- A2. Row count per table, business rows versus quarantined rows, side by side.
--     EXPECT: emsc 353, psgw 93, sgsn 175048, tap310 4. Total 175498.
--             Every quarantine count is 0.
--     WHY   : This is the headline health check. A non-zero quarantine count means
--             records failed a data-quality rule and were diverted, not lost.
SELECT 'emsc_cdr_raw'   AS table_name,
       (SELECT count(*) FROM emsc_cdr_raw)             AS good_rows,
       (SELECT count(*) FROM emsc_cdr_raw_quarantine)  AS quarantined_rows
UNION ALL
SELECT 'psgw_cdr_raw',
       (SELECT count(*) FROM psgw_cdr_raw),
       (SELECT count(*) FROM psgw_cdr_raw_quarantine)
UNION ALL
SELECT 'sgsn_cdr_raw',
       (SELECT count(*) FROM sgsn_cdr_raw),
       (SELECT count(*) FROM sgsn_cdr_raw_quarantine)
UNION ALL
SELECT 'tap310_raw',
       (SELECT count(*) FROM tap310_raw),
       (SELECT count(*) FROM tap310_raw_quarantine)
ORDER  BY good_rows DESC;


-- A3. Files consumed per table, and the window they were modified in.
--     EXPECT: emsc 3 files, psgw 3, sgsn 1, tap310 4. Eleven files in total.
--     WHY   : Ties the row counts back to physical files. If a file was dropped on the
--             floor, the distinct path count is the first place it shows up.
SELECT 'emsc_cdr_raw' AS table_name, count(DISTINCT path) AS files,
       min(modificationTime) AS first_file, max(modificationTime) AS last_file
FROM   emsc_cdr_raw
UNION ALL
SELECT 'psgw_cdr_raw', count(DISTINCT path), min(modificationTime), max(modificationTime) FROM psgw_cdr_raw
UNION ALL
SELECT 'sgsn_cdr_raw', count(DISTINCT path), min(modificationTime), max(modificationTime) FROM sgsn_cdr_raw
UNION ALL
SELECT 'tap310_raw',   count(DISTINCT path), min(modificationTime), max(modificationTime) FROM tap310_raw
ORDER  BY files DESC;


-- A4. Decode errors across all four decoders.
--     EXPECT: 0 in every row.
--     WHY   : _asn1_decode_error is populated when the ASN.1 decoder could not parse a
--             record. Any non-zero value here means the schema and the binary disagree,
--             and the affected rows must not be trusted.
SELECT 'emsc_cdr_raw' AS table_name, count(*) AS decode_errors FROM emsc_cdr_raw WHERE _asn1_decode_error IS NOT NULL
UNION ALL SELECT 'psgw_cdr_raw', count(*) FROM psgw_cdr_raw WHERE _asn1_decode_error IS NOT NULL
UNION ALL SELECT 'sgsn_cdr_raw', count(*) FROM sgsn_cdr_raw WHERE _asn1_decode_error IS NOT NULL
UNION ALL SELECT 'tap310_raw',   count(*) FROM tap310_raw   WHERE _asn1_decode_error IS NOT NULL;


-- =====================================================================================
-- PART B - STRUCTURE. Understanding the CHOICE column, which is the key to UC7.
-- =====================================================================================
--
-- ASN.1 CHOICE explained in one paragraph:
--   A CDR file does not contain one record shape. It contains a stream of records, and
--   each record is ONE OF several possible shapes. ASN.1 calls this a CHOICE. The
--   decoder therefore produces one struct column per possible shape, and populates
--   exactly one of them per row, leaving the rest NULL. The _choice column names the
--   one that was populated. You must always filter or COALESCE on _choice. Selecting a
--   struct column blindly returns mostly NULLs, and that is correct behaviour, not a bug.
--
-- =====================================================================================

-- B1. Which CHOICE variant is actually present, per table.
--     EXPECT: sgsn   -> sgsnPDPRecord 175048 (one variant only)
--             psgw   -> sGWRecord 68, pGWRecord 25
--             emsc   -> uMTSGSMPLMNCallDataRecord 350, compositeCallDataRecord 3
--             tap310 -> transferBatch 4
--     WHY   : Tells you which struct columns are worth querying, and which are dead
--             weight for this particular data set.
SELECT 'sgsn_cdr_raw' AS table_name, _choice, count(*) AS records FROM sgsn_cdr_raw GROUP BY 1, 2
UNION ALL SELECT 'psgw_cdr_raw', _choice, count(*) FROM psgw_cdr_raw GROUP BY 1, 2
UNION ALL SELECT 'emsc_cdr_raw', _choice, count(*) FROM emsc_cdr_raw GROUP BY 1, 2
UNION ALL SELECT 'tap310_raw',   _choice, count(*) FROM tap310_raw   GROUP BY 1, 2
ORDER  BY table_name, records DESC;


-- B2. Column budget per table: business columns versus framework columns.
--     EXPECT: emsc 16 columns (9 business + 7 framework)
--             psgw 27 (20 + 7), sgsn 27 (20 + 7), tap310 16 (9 + 7)
--     WHY    : Every table carries exactly seven __framework_ columns. They are lineage,
--              not payload. Knowing the split stops people from mistaking plumbing for data.
SELECT table_name,
       count(*)                                                              AS total_columns,
       count_if(column_name NOT LIKE '\\_\\_framework\\_%' ESCAPE '\\')      AS business_columns,
       count_if(column_name LIKE '\\_\\_framework\\_%' ESCAPE '\\')          AS framework_columns
FROM   br_digital_poc.information_schema.columns
WHERE  table_schema = 'bronze'
  AND  table_name IN ('emsc_cdr_raw', 'psgw_cdr_raw', 'sgsn_cdr_raw', 'tap310_raw')
GROUP  BY table_name
ORDER  BY table_name;


-- B3. The seven framework lineage columns, and what each one answers.
--     EXPECT: 7 rows, identical set for all four tables.
--     WHY    : These columns are how you answer "where did this row come from" without
--              joining to anything.
SELECT column_name,
       data_type,
       CASE column_name
         WHEN '__framework_ingestion_timestamp_utc'      THEN 'When the framework wrote this row'
         WHEN '__framework_source_file_name'             THEN 'Which physical file the row came from'
         WHEN '__framework_source_file_size'             THEN 'Size of that file in bytes'
         WHEN '__framework_source_file_modification_time' THEN 'When that file last changed on the volume'
         WHEN '__framework_source_file_metadata_headers' THEN 'Any additional file metadata captured'
         WHEN '__framework_pipeline_run_id'              THEN 'Which pipeline update produced the row'
         WHEN '__framework_record_id'                    THEN 'Stable identifier for the record'
       END AS what_it_answers
FROM   br_digital_poc.information_schema.columns
WHERE  table_schema = 'bronze'
  AND  table_name   = 'sgsn_cdr_raw'
  AND  column_name LIKE '\\_\\_framework\\_%' ESCAPE '\\'
ORDER  BY column_name;


-- =====================================================================================
-- PART C - SOURCE DATA ANALYSIS. The join keys, and how good they are.
-- =====================================================================================
--
-- The four candidate join keys in a mobile network, in order of usefulness:
--   servedIMSI        - identifies the SIM. Stable. The natural subscriber key.
--   servedMSISDN      - the phone number. Can be reassigned between subscribers.
--   chargingID        - identifies one data session. Unique per PDP context.
--   recordOpeningTime - when the session started. Used for time-window joins.
--
-- IMPORTANT ENCODING NOTE
--   IMSI and MSISDN are stored as TBCD-encoded binary, surfaced as base64 text such as
--   'MjSQAABxgPQ='. They are NOT human-readable digits. Two consequences:
--     - Joining on them works, because equal subscribers give equal encodings.
--     - Reading them by eye does not work. A silver-layer decode step is required
--       before any business user sees them.
--
-- =====================================================================================

-- C1. SGSN key quality. Nulls, cardinality and duplication.
--     EXPECT: 175048 rows, 0 nulls, 106384 distinct IMSI, 123383 distinct chargingID.
--     WHY    : 106384 distinct IMSIs across 175048 rows means the average subscriber
--              has about 1.6 sessions. That is a healthy many-to-one shape, and it tells
--              you a join on IMSI alone will fan out. Use IMSI plus chargingID for a
--              session-level join.
SELECT count(*)                                       AS total_rows,
       count(sgsnPDPRecord.servedIMSI)                 AS imsi_not_null,
       count(*) - count(sgsnPDPRecord.servedIMSI)      AS imsi_null,
       count(DISTINCT sgsnPDPRecord.servedIMSI)        AS distinct_imsi,
       count(DISTINCT sgsnPDPRecord.servedMSISDN)      AS distinct_msisdn,
       count(DISTINCT sgsnPDPRecord.chargingID)        AS distinct_charging_id,
       round(count(*) / count(DISTINCT sgsnPDPRecord.servedIMSI), 2) AS avg_sessions_per_subscriber
FROM   sgsn_cdr_raw;


-- C2. PSGW key quality, across both of its CHOICE variants.
--     EXPECT: 93 rows, 4 distinct IMSI, 52 distinct chargingID.
--     WHY    : PSGW is a small sample. Four subscribers only. This matters enormously
--              for the join analysis in Part D, and is the reason the joins return zero.
SELECT count(*)                                                          AS total_rows,
       count(DISTINCT coalesce(sGWRecord.servedIMSI,   pGWRecord.servedIMSI))   AS distinct_imsi,
       count(DISTINCT coalesce(sGWRecord.servedMSISDN, pGWRecord.servedMSISDN)) AS distinct_msisdn,
       count(DISTINCT coalesce(sGWRecord.chargingID,   pGWRecord.chargingID))   AS distinct_charging_id
FROM   psgw_cdr_raw;


-- C3. Top ten busiest subscribers in SGSN, by session count.
--     WHY : A simple, recognisable business question. Also demonstrates that GROUP BY on
--           a nested struct field works exactly like a normal column.
SELECT sgsnPDPRecord.servedIMSI                AS imsi_base64,
       count(*)                                 AS sessions,
       count(DISTINCT sgsnPDPRecord.chargingID) AS distinct_charging_ids,
       min(sgsnPDPRecord.recordOpeningTime)     AS first_session,
       max(sgsnPDPRecord.recordOpeningTime)     AS last_session
FROM   sgsn_cdr_raw
GROUP  BY 1
ORDER  BY sessions DESC
LIMIT  10;


-- C4. EMSC voice-call fields. Four levels of nesting.
--     WHY : Shows the real depth of the EMSC structure. The path is
--           uMTSGSMPLMNCallDataRecord -> callDataRecord -> mSOriginating -> field.
--           Anyone writing silver-layer SQL must know this path exists.
SELECT uMTSGSMPLMNCallDataRecord.callDataRecord._choice           AS call_type,
       count(*)                                                    AS records
FROM   emsc_cdr_raw
WHERE  uMTSGSMPLMNCallDataRecord IS NOT NULL
GROUP  BY 1
ORDER  BY records DESC;


-- C5. TAP 3.10 roaming batch headers. Who sent the file, and to whom.
--     EXPECT: 4 rows, one per TAP file.
--     WHY    : TAP files are inter-operator settlement files. The sender and recipient
--              identify the two operators settling with each other.
SELECT transferBatch.batchControlInfo.sender                     AS sender_operator,
       transferBatch.batchControlInfo.recipient                  AS recipient_operator,
       transferBatch.batchControlInfo.fileSequenceNumber         AS file_sequence,
       transferBatch.batchControlInfo.specificationVersionNumber AS tap_spec_version,
       transferBatch.batchControlInfo.releaseVersionNumber       AS tap_release_version
FROM   tap310_raw;


-- =====================================================================================
-- PART D - JOIN ANALYSIS. The honest answer.
-- =====================================================================================
--
-- READ THIS BEFORE RUNNING PART D
--
--   UC7 as built performs NO JOINS. It has four ingestion flows, zero transformation
--   flows and zero reconciliation flows. Each network element is decoded into its own
--   bronze table, independently. That is a deliberate scope decision, not an omission:
--   bronze must faithfully mirror the source, and correlation belongs in silver.
--
--   The queries below therefore serve two purposes:
--     1. They prove the join keys are structurally compatible across elements.
--     2. They measure the ACTUAL overlap on the current data, which is ZERO.
--
--   Zero overlap is a property of the sample data, not a defect. SGSN carries 106384
--   subscribers, PSGW carries 4, and they are different subscribers. Any silver-layer
--   correlation built on this sample will return no matched rows, and a team that does
--   not know this in advance will waste days debugging correct SQL.
--
-- =====================================================================================

-- D1. THE DECISIVE TEST. Subscriber overlap between SGSN and PSGW on IMSI.
--     EXPECT: sgsn_distinct 106384, psgw_distinct 4, overlap 0.
--     WHY    : If overlap is zero, no inner join between these two tables can ever
--              produce a row, however well written. Always run this before building a join.
WITH sgsn_keys AS (
    SELECT DISTINCT sgsnPDPRecord.servedIMSI AS imsi
    FROM   sgsn_cdr_raw
    WHERE  sgsnPDPRecord.servedIMSI IS NOT NULL
),
psgw_keys AS (
    SELECT DISTINCT coalesce(sGWRecord.servedIMSI, pGWRecord.servedIMSI) AS imsi
    FROM   psgw_cdr_raw
    WHERE  coalesce(sGWRecord.servedIMSI, pGWRecord.servedIMSI) IS NOT NULL
)
SELECT (SELECT count(*) FROM sgsn_keys)                          AS sgsn_distinct_imsi,
       (SELECT count(*) FROM psgw_keys)                          AS psgw_distinct_imsi,
       (SELECT count(*) FROM sgsn_keys JOIN psgw_keys USING (imsi)) AS overlapping_imsi;


-- D2. The same test on chargingID, the session-level key.
--     EXPECT: sgsn 123383, psgw 52, overlap 0.
--     WHY    : Confirms D1 independently. Two different keys, both showing zero overlap,
--              is conclusive. This is not a key-choice problem, it is a data-sample fact.
WITH sgsn_chg AS (
    SELECT DISTINCT sgsnPDPRecord.chargingID AS charging_id FROM sgsn_cdr_raw
),
psgw_chg AS (
    SELECT DISTINCT coalesce(sGWRecord.chargingID, pGWRecord.chargingID) AS charging_id FROM psgw_cdr_raw
)
SELECT (SELECT count(*) FROM sgsn_chg)                                AS sgsn_distinct,
       (SELECT count(*) FROM psgw_chg)                                AS psgw_distinct,
       (SELECT count(*) FROM sgsn_chg JOIN psgw_chg USING (charging_id)) AS overlapping;


-- D3. The session correlation join, written correctly, for when real data arrives.
--     EXPECT on current data: 0 rows. This is correct and expected.
--     WHY    : This is the join a silver layer would actually use. It is written here so
--              the team has a tested starting point rather than a blank page.
--              Key choice: IMSI plus chargingID identifies one session for one subscriber.
SELECT s.sgsnPDPRecord.servedIMSI          AS imsi,
       s.sgsnPDPRecord.chargingID          AS charging_id,
       s.sgsnPDPRecord.recordOpeningTime   AS sgsn_session_start,
       s.sgsnPDPRecord.duration            AS sgsn_duration_secs,
       p.sGWRecord.recordOpeningTime       AS psgw_session_start,
       p.sGWRecord.duration                AS psgw_duration_secs,
       p.sGWRecord.accessPointNameNI       AS apn
FROM   sgsn_cdr_raw s
INNER  JOIN psgw_cdr_raw p
       ON  s.sgsnPDPRecord.servedIMSI = p.sGWRecord.servedIMSI
       AND s.sgsnPDPRecord.chargingID = p.sGWRecord.chargingID
LIMIT  100;


-- D4. LEFT JOIN diagnostic. Proves the join runs and quantifies the miss rate.
--     EXPECT: matched 0, unmatched 175048, match_rate_pct 0.00.
--     WHY    : An INNER JOIN returning nothing is ambiguous. It could mean broken SQL or
--              no overlapping data. A LEFT JOIN with a counted match rate removes that
--              ambiguity, and is the correct diagnostic to run first.
SELECT count(*)                                          AS sgsn_rows,
       count(p.sGWRecord.chargingID)                     AS matched_rows,
       count(*) - count(p.sGWRecord.chargingID)          AS unmatched_rows,
       round(100.0 * count(p.sGWRecord.chargingID) / count(*), 2) AS match_rate_pct
FROM   sgsn_cdr_raw s
LEFT   JOIN psgw_cdr_raw p
       ON  s.sgsnPDPRecord.servedIMSI = p.sGWRecord.servedIMSI
       AND s.sgsnPDPRecord.chargingID = p.sGWRecord.chargingID;


-- D5. Self-join within SGSN. This one DOES return data.
--     WHY : Demonstrates a working join on real UC7 data, so the team can see the pattern
--           succeed. It finds subscribers with more than one session, which is a genuine
--           business question, answerable today without any silver layer.
SELECT a.sgsnPDPRecord.servedIMSI        AS imsi,
       a.sgsnPDPRecord.chargingID        AS session_a,
       b.sgsnPDPRecord.chargingID        AS session_b,
       a.sgsnPDPRecord.recordOpeningTime AS start_a,
       b.sgsnPDPRecord.recordOpeningTime AS start_b
FROM   sgsn_cdr_raw a
INNER  JOIN sgsn_cdr_raw b
       ON  a.sgsnPDPRecord.servedIMSI = b.sgsnPDPRecord.servedIMSI
       AND a.sgsnPDPRecord.chargingID < b.sgsnPDPRecord.chargingID
LIMIT  20;


-- D6. Union-based correlation. The pattern that DOES work across elements.
--     WHY : When subscriber overlap is zero, a UNION view is more useful than a JOIN.
--           It produces one unified session list across elements, which is exactly what
--           a silver layer should publish. Note every element keeps its own identity.
SELECT 'SGSN' AS network_element,
       sgsnPDPRecord.servedIMSI          AS imsi,
       sgsnPDPRecord.chargingID          AS charging_id,
       sgsnPDPRecord.recordOpeningTime   AS session_start,
       sgsnPDPRecord.duration            AS duration_secs,
       sgsnPDPRecord.accessPointNameNI   AS apn
FROM   sgsn_cdr_raw
UNION ALL
SELECT 'PSGW-SGW',
       sGWRecord.servedIMSI, sGWRecord.chargingID,
       sGWRecord.recordOpeningTime, sGWRecord.duration, sGWRecord.accessPointNameNI
FROM   psgw_cdr_raw WHERE sGWRecord IS NOT NULL
UNION ALL
SELECT 'PSGW-PGW',
       pGWRecord.servedIMSI, pGWRecord.chargingID,
       pGWRecord.recordOpeningTime, pGWRecord.duration, pGWRecord.accessPointNameNI
FROM   psgw_cdr_raw WHERE pGWRecord IS NOT NULL
LIMIT  50;


-- =====================================================================================
-- PART E - TARGET DATA AND EXPORT ANALYSIS.
-- =====================================================================================

-- E1. Source-to-target reconciliation, per file.
--     WHY : The auditor's question. For each physical file, how many rows reached bronze.
--           A file present on the volume but absent here is silent data loss.
SELECT __framework_source_file_name        AS source_file,
       count(*)                             AS rows_landed,
       min(__framework_ingestion_timestamp_utc) AS ingested_at,
       max(__framework_source_file_size)    AS file_size_bytes
FROM   sgsn_cdr_raw
GROUP  BY 1
ORDER  BY rows_landed DESC;


-- E2. All four elements reconciled in one result.
--     EXPECT: 11 files, 175498 rows in total.
--     WHY    : Single-screen answer to "did everything arrive". Hand this to an auditor.
SELECT 'emsc_cdr_raw' AS table_name, count(DISTINCT __framework_source_file_name) AS files, count(*) AS rows_landed FROM emsc_cdr_raw
UNION ALL SELECT 'psgw_cdr_raw', count(DISTINCT __framework_source_file_name), count(*) FROM psgw_cdr_raw
UNION ALL SELECT 'sgsn_cdr_raw', count(DISTINCT __framework_source_file_name), count(*) FROM sgsn_cdr_raw
UNION ALL SELECT 'tap310_raw',   count(DISTINCT __framework_source_file_name), count(*) FROM tap310_raw;


-- E3. Ingestion latency. File modification time to bronze write time.
--     WHY : Measures how long the pipeline took to pick up and process each file.
--           Useful as a service-level indicator once UC7 runs on a schedule.
SELECT __framework_source_file_name                      AS source_file,
       min(__framework_source_file_modification_time)    AS file_modified_at,
       min(__framework_ingestion_timestamp_utc)          AS bronze_written_at,
       round(( unix_timestamp(min(__framework_ingestion_timestamp_utc))
             - unix_timestamp(min(__framework_source_file_modification_time)) ) / 60.0, 2)
                                                          AS latency_minutes
FROM   sgsn_cdr_raw
GROUP  BY 1
ORDER  BY latency_minutes DESC;


-- E4. Governance tags applied to UC7 tables.
--     WHY : Tags drive access policy. This proves the governance task actually ran.
--           An empty result means tags were never applied, even if the pipeline was green.
--     NOTE: information_schema is CATALOG-SCOPED. The three-part name below is mandatory.
--           Querying an unqualified information_schema returns rows for the wrong catalog
--           and looks convincingly like "tags are not supported here".
SELECT schema_name,
       table_name,
       tag_name,
       tag_value
FROM   br_digital_poc.information_schema.table_tags
WHERE  table_name IN ('emsc_cdr_raw', 'psgw_cdr_raw', 'sgsn_cdr_raw', 'tap310_raw')
ORDER  BY table_name, tag_name;


-- E5. A ready-to-export flattened extract from SGSN.
--     WHY : What a downstream consumer, a BI tool or a CSV export would actually select.
--           Nested structs are flattened to plain columns, and framework columns are kept
--           deliberately so the extract remains auditable.
SELECT sgsnPDPRecord.servedIMSI                AS imsi_base64,
       sgsnPDPRecord.servedMSISDN              AS msisdn_base64,
       sgsnPDPRecord.chargingID                AS charging_id,
       sgsnPDPRecord.accessPointNameNI         AS apn,
       sgsnPDPRecord.recordOpeningTime         AS session_start,
       sgsnPDPRecord.duration                  AS duration_secs,
       sgsnPDPRecord.causeForRecClosing        AS close_cause,
       sgsnPDPRecord.rATType                   AS radio_access_type,
       sgsnPDPRecord.sgsnAddress               AS sgsn_address,
       __framework_source_file_name            AS source_file,
       __framework_ingestion_timestamp_utc     AS ingested_at
FROM   sgsn_cdr_raw
ORDER  BY session_start DESC
LIMIT  1000;


-- =====================================================================================
-- PART F - DATA QUALITY SCORECARD. Run this one for a demo.
-- =====================================================================================

-- F1. One-screen health scorecard. Every line should read PASS.
--     WHY : Combines the individual checks above into a single go or no-go answer.
WITH checks AS (
    SELECT 'All four tables published'  AS check_name,
           (SELECT count(*) FROM br_digital_poc.information_schema.tables
             WHERE table_schema = 'bronze'
               AND table_name IN ('emsc_cdr_raw','psgw_cdr_raw','sgsn_cdr_raw','tap310_raw')) AS actual,
           4 AS expected
    UNION ALL
    SELECT 'Zero quarantined records',
           (SELECT count(*) FROM emsc_cdr_raw_quarantine)
         + (SELECT count(*) FROM psgw_cdr_raw_quarantine)
         + (SELECT count(*) FROM sgsn_cdr_raw_quarantine)
         + (SELECT count(*) FROM tap310_raw_quarantine), 0
    UNION ALL
    SELECT 'Zero ASN.1 decode errors',
           (SELECT count(*) FROM emsc_cdr_raw WHERE _asn1_decode_error IS NOT NULL)
         + (SELECT count(*) FROM psgw_cdr_raw WHERE _asn1_decode_error IS NOT NULL)
         + (SELECT count(*) FROM sgsn_cdr_raw WHERE _asn1_decode_error IS NOT NULL)
         + (SELECT count(*) FROM tap310_raw   WHERE _asn1_decode_error IS NOT NULL), 0
    UNION ALL
    SELECT 'Every row carries a source file',
           (SELECT count(*) FROM sgsn_cdr_raw WHERE __framework_source_file_name IS NULL), 0
    UNION ALL
    SELECT 'Every SGSN row has an IMSI',
           (SELECT count(*) FROM sgsn_cdr_raw WHERE sgsnPDPRecord.servedIMSI IS NULL), 0
)
SELECT check_name,
       expected,
       actual,
       CASE WHEN actual = expected THEN 'PASS' ELSE 'FAIL' END AS result
FROM   checks
ORDER  BY result DESC, check_name;


-- =====================================================================================
-- PART G - PIPELINE EVENT LOG. Prove the counts from the pipeline's own record.
-- =====================================================================================

-- G1. Rows written per dataset, taken from the DLT event log rather than the tables.
--     WHY : Independent corroboration. If the table count and the event-log count agree,
--           nothing was written or removed outside the pipeline.
--     NOTE: Replace the pipeline id if UC7 is redeployed.
SELECT origin.flow_name                                      AS dataset,
       details:flow_progress.metrics.num_output_rows         AS rows_written,
       timestamp
FROM   event_log('927a6e24-757f-495c-8a94-d807b74c4128')
WHERE  event_type = 'flow_progress'
  AND  details:flow_progress.metrics.num_output_rows IS NOT NULL
ORDER  BY timestamp DESC
LIMIT  40;


-- G2. Expectation and data-quality outcomes recorded by the pipeline.
--     WHY : Shows which DQ rules ran, how many rows passed and how many were dropped.
SELECT timestamp,
       origin.flow_name                          AS dataset,
       details:flow_progress.data_quality        AS data_quality
FROM   event_log('927a6e24-757f-495c-8a94-d807b74c4128')
WHERE  event_type = 'flow_progress'
  AND  details:flow_progress.data_quality IS NOT NULL
ORDER  BY timestamp DESC
LIMIT  20;


-- =====================================================================================
-- END OF QUERY PACK
--
-- SUMMARY OF WHAT THIS PACK PROVES
--   1. Four network elements decode cleanly. 175498 rows, 11 files, zero errors.
--   2. Every table is CHOICE-based. Always filter on _choice.
--   3. Join keys are structurally sound. IMSI, MSISDN and chargingID all exist.
--   4. Cross-element overlap on the current sample is ZERO. Joins are correct but
--      return no rows. Use UNION for a unified view until overlapping data arrives.
--   5. Lineage is complete. Every row traces to a file, a run and a timestamp.
-- =====================================================================================
