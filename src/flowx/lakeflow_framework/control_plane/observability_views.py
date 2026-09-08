"""DDL text for the FlowX observability semantic layer -- the views that join the framework's
own control metadata to the Databricks **system tables**.

WHY THIS MODULE EXISTS. The framework's runtime observability is *export-oriented*: flow row
counts, expectation pass/fail, DQ quarantine counts and SCD change counts are computed and then
shipped out as OTLP JSON, Volume files, or driver logs (see ``observability/``). Almost none of it
lands in a queryable Delta table. Meanwhile the platform already records the operational half --
run durations, result states, retries, DBU cost, table lineage -- in ``system.lakeflow``,
``system.billing`` and ``system.access``. Neither half is useful alone: the system tables know a
pipeline ran for 94 seconds but not that it is ``dfg_uc6_ea_flood_warning`` implementing 11 flows
with 5 DQ rules; the control tables know the reverse. These views are the join.

THE JOIN KEY. ``system.lakeflow.pipelines.configuration`` is a MAP, and every FlowX pipeline
resource already sets ``dataflow.group.id`` in its ``configuration:`` block. So
``configuration['dataflow.group.id']`` recovers the FlowX identity of a pipeline with no change to
any pipeline. That is the spine of every view below.

The job side is weaker on purpose: the framework passes ``dataflow_group_id`` as a *notebook task
base_parameter*, and those do **not** surface in ``system.lakeflow.job_task_run_timeline
.task_parameters`` (verified empty on a live workspace). Job-level attribution therefore reads a
``dataflow_group_id`` **tag**, which ``resources/**`` now sets. Untagged jobs fall back to a name
match and are reported as ``attribution = 'name_match'`` rather than being silently dropped --
see ``get_job_runs_view_ddl``.

WHY VIEWS, AND WHY HERE. Two consumers need identical semantics: the AI/BI dashboard
(``databricks-bi/*.lvdash.json``) and the Genie space (``resources/flowx_genie/``). A Genie space
answers questions well only when it is pointed at a small set of well-named, well-commented
objects rather than at raw system tables, and a dashboard whose SQL is copy-pasted into a Genie
space drifts from it within one release. Defining the join once, as commented views, gives both a
single source of truth. Column ``COMMENT``s are not decoration here -- Genie reads them as the
semantic model, so they are written to be read by an LLM.

Consistent with ``ddl_definitions.py``, these are pure string-building functions: no
``spark.sql`` execution happens in this module. ``notebooks/01_setup/01_setup_control_tables.py``
is the only place that executes them.

NOTE ON BRACES. As in ``ddl_definitions.py``, any literal ``{`` or ``}`` inside an f-string DDL
body -- including inside a column COMMENT -- must be escaped as ``{{ }}``, or Python evaluates it
as a replacement field and every statement in the module breaks.
"""

from typing import Dict, List, Optional, Tuple

# The observability views live in their own schema rather than alongside the control tables.
# Keeping them out of `<catalog>.config` means a Genie space or a BI user can be granted SELECT on
# the whole observability surface without also being granted read access to the control tables,
# whose *_json / raw_spec_payload columns carry connection strings and credential-shaped config.
OBSERVABILITY_SCHEMA_SUFFIX = "observability"


def get_observability_schema_ddl(observability_schema: str) -> str:
    return (
        f"CREATE SCHEMA IF NOT EXISTS {observability_schema} "
        f"COMMENT 'FlowX observability semantic layer: views joining FlowX control metadata to "
        f"the Databricks system tables. Read-only; safe to grant to BI and Genie consumers.'"
    )


def get_dataflow_group_catalog_view_ddl(observability_schema: str, control_schema: str) -> str:
    """The dimension view: one row per dataflow group, with its composition and feature footprint.

    This is the view that answers "what is this dataflow group and what does it do", which is the
    single most common question asked of the Genie space. It deliberately pre-computes the feature
    flags (has_cdc, has_dq, has_quarantine, ...) rather than leaving an LLM to infer them from raw
    JSON blobs, and it exposes ``feature_summary`` as a human-readable sentence because Genie
    reproduces such a column verbatim in an answer far more reliably than it composes one.
    """
    return f"""
CREATE OR REPLACE VIEW {observability_schema}.v_dataflow_group_catalog (
    dataflow_group_id       COMMENT 'FlowX dataflow group identifier, e.g. dfg_uc6_ea_flood_warning. The primary business key for everything in this schema; join on this.',
    environment             COMMENT 'Target environment recorded at onboarding: dev, staging, prod.',
    catalog_name            COMMENT 'Unity Catalog catalog this group writes into.',
    is_active               COMMENT 'FALSE means the group is soft-disabled and the engine skips it. NOTE: deleting a flow from a spec does NOT deactivate it here.',
    ingestion_flow_count    COMMENT 'Number of Bronze ingestion flows (external reads) defined in this group.',
    transformation_flow_count COMMENT 'Number of Silver/Gold transformation flows (SQL steps) defined in this group.',
    reconciliation_flow_count COMMENT 'Number of source-to-target reconciliation flows defined in this group.',
    total_flow_count        COMMENT 'Total flows of all three kinds. Use this for "how big is this group".',
    source_types            COMMENT 'Distinct ingestion source connector types used, comma separated (e.g. autoloader, zerobus, asn1).',
    target_types            COMMENT 'Distinct target materialization types used, comma separated (e.g. streaming_table, materialized_view).',
    cdc_load_strategies     COMMENT 'Distinct CDC / load strategies used, comma separated (e.g. append, scd_type_1, scd_type_2, truncate_and_load).',
    target_tables           COMMENT 'Fully-qualified tables this group writes, comma separated.',
    has_cdc                 COMMENT 'TRUE when any flow uses an SCD or CDC load strategy rather than plain append.',
    has_dq                  COMMENT 'TRUE when any flow declares data-quality expectations.',
    has_quarantine          COMMENT 'TRUE when any flow routes failing records to a quarantine table instead of dropping or failing.',
    has_reconciliation      COMMENT 'TRUE when the group defines at least one reconciliation flow.',
    has_governance_tags     COMMENT 'TRUE when any flow attaches Unity Catalog governance tags to its target.',
    feature_summary         COMMENT 'Human-readable one-line summary of the capabilities this group exercises. Quote this directly when asked what features a group uses.',
    onboarded_by            COMMENT 'Identity that most recently onboarded this group.',
    last_onboarded_at       COMMENT 'Timestamp of the most recent successful onboarding action.',
    spec_version            COMMENT 'Onboarding spec version recorded on the most recent onboarding.',
    created_at              COMMENT 'When the group was first onboarded.',
    updated_at              COMMENT 'When the group control row was last updated.'
)
COMMENT 'One row per FlowX dataflow group: what it contains, which framework features it uses, and who onboarded it. The dimension table of the observability layer -- start here to identify a group, then join to v_pipeline_updates or v_flow_metrics for run behaviour.'
AS
WITH ing AS (
    SELECT
        dataflow_group_id,
        COUNT(*)                                                    AS flow_count,
        CONCAT_WS(', ', ARRAY_SORT(COLLECT_SET(source_type)))       AS source_types,
        CONCAT_WS(', ', ARRAY_SORT(COLLECT_SET(target_type)))       AS target_types,
        CONCAT_WS(', ', ARRAY_SORT(COLLECT_SET(cdc_load_strategy))) AS cdc_strategies,
        CONCAT_WS(', ', ARRAY_SORT(COLLECT_SET(
            CONCAT(target_catalog, '.', target_schema, '.', target_table)))) AS target_tables,
        -- An empty JSON object is an EMPTY dq_config, which several onboarded flows carry: treating a
        -- non-empty STRING as "has DQ" reported has_dq = true for every group. Require the JSON
        -- to actually contain a rules key. LOWER() because these values are not case-normalised.
        MAX(CASE WHEN dq_config_json IS NOT NULL AND TRIM(dq_config_json) NOT IN ('', '{{}}')
                 THEN 1 ELSE 0 END)                                                          AS has_dq,
        MAX(CASE WHEN LOWER(dq_config_json) LIKE '%quarantine%' THEN 1 ELSE 0 END)           AS has_quarantine,
        MAX(CASE WHEN governance_tags_json IS NOT NULL
                      AND TRIM(governance_tags_json) NOT IN ('', '{{}}')
                 THEN 1 ELSE 0 END)                                                          AS has_tags
    FROM {control_schema}.ingestion_flow_spec
    GROUP BY dataflow_group_id
),
xfm AS (
    SELECT
        dataflow_group_id,
        COUNT(*)                                                    AS flow_count,
        CONCAT_WS(', ', ARRAY_SORT(COLLECT_SET(target_type)))       AS target_types,
        CONCAT_WS(', ', ARRAY_SORT(COLLECT_SET(cdc_load_strategy))) AS cdc_strategies,
        CONCAT_WS(', ', ARRAY_SORT(COLLECT_SET(
            CONCAT(target_catalog, '.', target_schema, '.', target_table)))) AS target_tables,
        -- An empty JSON object is an EMPTY dq_config, which several onboarded flows carry: treating a
        -- non-empty STRING as "has DQ" reported has_dq = true for every group. Require the JSON
        -- to actually contain a rules key. LOWER() because these values are not case-normalised.
        MAX(CASE WHEN dq_config_json IS NOT NULL AND TRIM(dq_config_json) NOT IN ('', '{{}}')
                 THEN 1 ELSE 0 END)                                                          AS has_dq,
        MAX(CASE WHEN LOWER(dq_config_json) LIKE '%quarantine%' THEN 1 ELSE 0 END)           AS has_quarantine,
        MAX(CASE WHEN governance_tags_json IS NOT NULL
                      AND TRIM(governance_tags_json) NOT IN ('', '{{}}')
                 THEN 1 ELSE 0 END)                                                          AS has_tags
    FROM {control_schema}.transformation_flow_spec
    GROUP BY dataflow_group_id
),
rec AS (
    SELECT dataflow_group_id, COUNT(*) AS flow_count
    FROM {control_schema}.reconciliation_flow_spec
    GROUP BY dataflow_group_id
),
aud AS (
    -- Most recent SUCCESSFUL onboarding per group. Ranking beats MAX(...) GROUP BY here because
    -- onboarded_by and spec_version must come from the *same* row as the timestamp.
    SELECT dataflow_group_id, onboarded_by, onboarded_at, spec_version
    FROM (
        SELECT
            dataflow_group_id, onboarded_by, onboarded_at, spec_version,
            ROW_NUMBER() OVER (PARTITION BY dataflow_group_id ORDER BY onboarded_at DESC) AS rn
        FROM {control_schema}.onboarding_audit_log
        WHERE status = 'SUCCESS'
    ) WHERE rn = 1
),
base AS (
    SELECT
        dg.dataflow_group_id,
        dg.environment,
        dg.catalog_name,
        dg.is_active,
        COALESCE(ing.flow_count, 0) AS ingestion_flow_count,
        COALESCE(xfm.flow_count, 0) AS transformation_flow_count,
        COALESCE(rec.flow_count, 0) AS reconciliation_flow_count,
        COALESCE(ing.flow_count, 0) + COALESCE(xfm.flow_count, 0)
            + COALESCE(rec.flow_count, 0) AS total_flow_count,
        ing.source_types,
        -- Merging the ingestion and transformation lists needs care: when one side has no rows
        -- its COALESCE(..., '') yields '', and SPLIT('', ', ') returns [''] -- a single EMPTY
        -- element that ARRAY_COMPACT does NOT drop (it only removes NULLs). That empty element
        -- then sorts first and CONCAT_WS renders it as a leading ", ". Filtering the array for
        -- non-empty entries is what actually fixes it.
        CONCAT_WS(', ', ARRAY_SORT(ARRAY_DISTINCT(FILTER(FLATTEN(ARRAY(
            SPLIT(COALESCE(ing.target_types, ''), ', '),
            SPLIT(COALESCE(xfm.target_types, ''), ', '))),
            x -> x IS NOT NULL AND x <> '')))) AS target_types,
        CONCAT_WS(', ', ARRAY_SORT(ARRAY_DISTINCT(FILTER(FLATTEN(ARRAY(
            SPLIT(COALESCE(ing.cdc_strategies, ''), ', '),
            SPLIT(COALESCE(xfm.cdc_strategies, ''), ', '))),
            x -> x IS NOT NULL AND x <> '')))) AS cdc_load_strategies,
        CONCAT_WS(', ', ARRAY_SORT(ARRAY_DISTINCT(FILTER(FLATTEN(ARRAY(
            SPLIT(COALESCE(ing.target_tables, ''), ', '),
            SPLIT(COALESCE(xfm.target_tables, ''), ', '))),
            x -> x IS NOT NULL AND x <> '')))) AS target_tables,
        COALESCE(
            GREATEST(COALESCE(ing.has_dq, 0), COALESCE(xfm.has_dq, 0)), 0) = 1 AS has_dq,
        COALESCE(
            GREATEST(COALESCE(ing.has_quarantine, 0), COALESCE(xfm.has_quarantine, 0)), 0) = 1
            AS has_quarantine,
        COALESCE(
            GREATEST(COALESCE(ing.has_tags, 0), COALESCE(xfm.has_tags, 0)), 0) = 1
            AS has_governance_tags,
        COALESCE(rec.flow_count, 0) > 0 AS has_reconciliation,
        aud.onboarded_by,
        aud.onboarded_at AS last_onboarded_at,
        aud.spec_version,
        dg.created_at,
        dg.updated_at
    FROM {control_schema}.dataflow_group_spec dg
    LEFT JOIN ing ON dg.dataflow_group_id = ing.dataflow_group_id
    LEFT JOIN xfm ON dg.dataflow_group_id = xfm.dataflow_group_id
    LEFT JOIN rec ON dg.dataflow_group_id = rec.dataflow_group_id
    LEFT JOIN aud ON dg.dataflow_group_id = aud.dataflow_group_id
)
SELECT
    dataflow_group_id,
    environment,
    catalog_name,
    is_active,
    ingestion_flow_count,
    transformation_flow_count,
    reconciliation_flow_count,
    total_flow_count,
    source_types,
    target_types,
    cdc_load_strategies,
    target_tables,
    -- "CDC" here means any strategy that is not a plain append: the SCD variants plus the
    -- snapshot/full-reload strategies all carry change-detection semantics a plain append lacks.
    -- LOWER() first -- the control tables store these UPPERCASE (SCD1, SCD2, APPEND,
    -- TRUNCATE_AND_LOAD), so a lowercase-only pattern silently reported has_cdc = false for
    -- every SCD group.
    LOWER(cdc_load_strategies) RLIKE '(scd|cdc|merge|truncate)' AS has_cdc,
    has_dq,
    has_quarantine,
    has_reconciliation,
    has_governance_tags,
    CONCAT_WS(' ',
        CONCAT(
            'Dataflow group ', dataflow_group_id, ' (', COALESCE(environment, 'unknown env'),
            ') defines ', CAST(total_flow_count AS STRING), ' flow(s): ',
            CAST(ingestion_flow_count AS STRING), ' ingestion, ',
            CAST(transformation_flow_count AS STRING), ' transformation, ',
            CAST(reconciliation_flow_count AS STRING), ' reconciliation.'),
        CASE WHEN source_types IS NOT NULL AND source_types <> ''
             THEN CONCAT('Ingests via ', source_types, '.') END,
        CASE WHEN cdc_load_strategies IS NOT NULL AND cdc_load_strategies <> ''
             THEN CONCAT('Load strategies: ', cdc_load_strategies, '.') END,
        CASE WHEN has_dq THEN 'Data-quality expectations are enforced.' END,
        CASE WHEN has_quarantine THEN 'Failing records are routed to a quarantine table.' END,
        CASE WHEN has_reconciliation THEN 'Source-to-target reconciliation is enabled.' END,
        CASE WHEN has_governance_tags THEN 'Unity Catalog governance tags are applied.' END,
        CASE WHEN NOT is_active THEN 'This group is currently INACTIVE.' END
    ) AS feature_summary,
    onboarded_by,
    last_onboarded_at,
    spec_version,
    created_at,
    updated_at
FROM base
"""


def get_pipeline_registry_view_ddl(observability_schema: str, control_schema: str) -> str:
    """Resolve FlowX dataflow group -> Lakeflow pipeline, and derive its UC event-log table name.

    This view is the reason the rest of the layer works. It relies on the fact that every FlowX
    pipeline resource sets ``dataflow.group.id`` in its ``configuration:`` block, which the
    platform surfaces as ``system.lakeflow.pipelines.configuration['dataflow.group.id']``.

    It also resolves ``event_log_table`` by *discovery* rather than by construction. When a
    pipeline publishes its event log to Unity Catalog the table is named
    ``event_log_<pipeline_id with hyphens replaced by underscores>``, but its catalog and schema
    are not recoverable from ``system.lakeflow.pipelines``: the ``settings`` struct exposes only
    ``photon``, ``development``, ``continuous``, ``serverless``, ``edition`` and ``channel`` --
    there is no ``catalog`` or ``target`` field to read (verified against a live workspace).
    So the name is matched against ``system.information_schema.tables`` on the id-derived suffix,
    which finds the table wherever it was published and leaves ``event_log_table`` NULL for a
    pipeline that publishes no event log.
    """
    return f"""
CREATE OR REPLACE VIEW {observability_schema}.v_pipeline_registry (
    dataflow_group_id COMMENT 'FlowX dataflow group this Lakeflow pipeline implements, read from the pipeline configuration key dataflow.group.id.',
    pipeline_id       COMMENT 'Lakeflow (Delta Live Tables) pipeline UUID. Join key to system.lakeflow.pipeline_update_timeline and to system.billing.usage.usage_metadata.dlt_pipeline_id.',
    pipeline_name     COMMENT 'Display name of the pipeline, e.g. 008_ldp_uc6_ea_flood_warning.',
    pipeline_type     COMMENT 'Pipeline type as reported by the platform (e.g. WORKSPACE).',
    control_catalog   COMMENT 'Unity Catalog catalog holding this group control tables, from the pipeline configuration key dataflow.control.catalog.',
    is_serverless     COMMENT 'TRUE when the pipeline runs on serverless compute.',
    is_continuous     COMMENT 'TRUE for a continuously-running pipeline, FALSE for triggered.',
    is_development    COMMENT 'TRUE when the pipeline is in development mode (compute is reused and retained between updates).',
    is_photon         COMMENT 'TRUE when Photon acceleration is enabled.',
    edition           COMMENT 'Pipeline product edition, e.g. ADVANCED.',
    channel           COMMENT 'Lakeflow runtime channel, e.g. CURRENT or PREVIEW.',
    run_as            COMMENT 'Identity the pipeline executes as.',
    created_by        COMMENT 'Identity that created the pipeline.',
    event_log_table   COMMENT 'Fully-qualified Unity Catalog table holding this pipeline event log, discovered from information_schema. NULL when the pipeline does not publish its event log to UC, in which case no per-flow metrics or DQ results are available for it. Query v_flow_metrics / v_dq_results rather than this table directly.',
    create_time       COMMENT 'When the pipeline was created.',
    change_time       COMMENT 'When the pipeline definition last changed.'
)
COMMENT 'Maps each FlowX dataflow group to the Lakeflow pipeline that implements it, via the pipeline configuration key dataflow.group.id. This is the bridge between FlowX control metadata and the Databricks system tables -- every run, cost and metric view joins through it.'
AS
WITH event_logs AS (
    -- Discover published event-log tables rather than constructing the name: the pipeline's
    -- publishing catalog/schema is not exposed by system.lakeflow.pipelines.settings, so match on
    -- the id-derived table name and take whatever catalog/schema it was actually published to.
    SELECT
        REPLACE(table_name, 'event_log_', '') AS pipeline_id_underscored,
        CONCAT(table_catalog, '.', table_schema, '.', table_name) AS event_log_table,
        ROW_NUMBER() OVER (
            PARTITION BY table_name ORDER BY table_catalog, table_schema) AS rn
    FROM system.information_schema.tables
    WHERE table_name LIKE 'event_log_%'
)
SELECT
    p.configuration['dataflow.group.id']       AS dataflow_group_id,
    p.pipeline_id,
    p.name                                     AS pipeline_name,
    p.pipeline_type,
    p.configuration['dataflow.control.catalog'] AS control_catalog,
    p.settings.serverless                      AS is_serverless,
    p.settings.continuous                      AS is_continuous,
    p.settings.development                     AS is_development,
    p.settings.photon                          AS is_photon,
    p.settings.edition                         AS edition,
    p.settings.channel                         AS channel,
    p.run_as,
    p.created_by,
    e.event_log_table,
    p.create_time,
    p.change_time
FROM system.lakeflow.pipelines p
LEFT JOIN event_logs e
       ON e.pipeline_id_underscored = REPLACE(p.pipeline_id, '-', '_')
      AND e.rn = 1
WHERE p.delete_time IS NULL
  AND p.configuration['dataflow.group.id'] IS NOT NULL
"""


def get_pipeline_updates_view_ddl(observability_schema: str) -> str:
    """Update-level pipeline run performance, attributed to a FlowX dataflow group.

    ``system.lakeflow.pipeline_update_timeline`` carries no duration column, so duration is
    derived from the period bounds. ``trigger_type = 'RETRY_ON_FAILURE'`` is surfaced explicitly:
    a burst of those rows is the signature of a pipeline failing and auto-retrying, which is
    otherwise easy to misread as legitimate activity.
    """
    return f"""
CREATE OR REPLACE VIEW {observability_schema}.v_pipeline_updates (
    dataflow_group_id  COMMENT 'FlowX dataflow group this update belongs to.',
    pipeline_name      COMMENT 'Name of the pipeline that ran.',
    pipeline_id        COMMENT 'Lakeflow pipeline UUID.',
    update_id          COMMENT 'Unique id of this pipeline update (one execution). Join key to flow-level metrics in v_flow_metrics.',
    update_type        COMMENT 'REFRESH for an incremental update, FULL_REFRESH when state was rebuilt from scratch.',
    result_state       COMMENT 'Terminal outcome: COMPLETED, FAILED, or CANCELED. NULL while the update is still running.',
    is_success         COMMENT 'TRUE when result_state = COMPLETED. Use for success-rate arithmetic.',
    is_failure         COMMENT 'TRUE when result_state = FAILED.',
    trigger_type       COMMENT 'What started the update: JOB_TASK (a Lakeflow job), API_CALL (manual or CLI), SCHEDULE, or RETRY_ON_FAILURE (an automatic retry of a failed update).',
    is_retry           COMMENT 'TRUE when this update is an automatic retry of a failed one. A cluster of these indicates a failing pipeline in a retry loop, not healthy throughput.',
    run_as_user_name   COMMENT 'Identity the update executed as.',
    started_at         COMMENT 'Update start timestamp.',
    ended_at           COMMENT 'Update end timestamp.',
    duration_seconds   COMMENT 'Wall-clock duration of the update in seconds, derived from the period bounds (the system table has no duration column).',
    duration_minutes   COMMENT 'Wall-clock duration in minutes, rounded to 2dp. Prefer this for charting.',
    is_full_refresh    COMMENT 'TRUE when this update was a full refresh. Full refreshes are expected to be much slower than incremental ones -- exclude them before comparing durations.',
    run_date           COMMENT 'Calendar date of the update start, for daily trend aggregation.'
)
COMMENT 'One row per Lakeflow pipeline update (one execution), attributed to its FlowX dataflow group, with derived duration and success flags. The primary source for pipeline run performance and reliability trends.'
AS
SELECT
    r.dataflow_group_id,
    r.pipeline_name,
    u.pipeline_id,
    u.update_id,
    u.update_type,
    u.result_state,
    u.result_state = 'COMPLETED'          AS is_success,
    u.result_state = 'FAILED'             AS is_failure,
    u.trigger_type,
    u.trigger_type = 'RETRY_ON_FAILURE'   AS is_retry,
    u.run_as_user_name,
    u.period_start_time                   AS started_at,
    u.period_end_time                     AS ended_at,
    CAST(TIMESTAMPDIFF(SECOND, u.period_start_time, u.period_end_time) AS BIGINT)
                                          AS duration_seconds,
    ROUND(TIMESTAMPDIFF(SECOND, u.period_start_time, u.period_end_time) / 60.0, 2)
                                          AS duration_minutes,
    u.update_type = 'FULL_REFRESH'        AS is_full_refresh,
    CAST(u.period_start_time AS DATE)     AS run_date
FROM system.lakeflow.pipeline_update_timeline u
INNER JOIN {observability_schema}.v_pipeline_registry r
        ON u.pipeline_id = r.pipeline_id
"""


def get_job_runs_view_ddl(observability_schema: str, control_schema: str) -> str:
    """Job-level run performance, attributed to a dataflow group by tag with a name-match fallback.

    Unlike the pipeline side, there is no reliable platform-surfaced parameter carrying the FlowX
    identity of a *job* run: the framework passes ``dataflow_group_id`` as a notebook task
    ``base_parameter``, and those do not appear in
    ``system.lakeflow.job_task_run_timeline.task_parameters``. So attribution is:

    1. ``system.lakeflow.jobs.tags['dataflow_group_id']`` -- exact, set by ``resources/**``.
    2. Otherwise, a case-insensitive containment match of the group id against the job name.

    The ``attribution`` column reports which path was taken so a viewer can tell a guaranteed
    attribution from a heuristic one, rather than the two being silently blended. Runs matching
    neither are still returned with a NULL group -- dropping them would hide framework jobs that
    predate the tag.
    """
    return f"""
CREATE OR REPLACE VIEW {observability_schema}.v_job_runs (
    dataflow_group_id COMMENT 'FlowX dataflow group this job run belongs to, resolved from the job tag dataflow_group_id or, failing that, from the job name. NULL when the job could not be attributed to any group.',
    attribution       COMMENT 'How dataflow_group_id was resolved: "tag" (exact, from the job tag) or "name_match" (heuristic, the group id appears in the job name). Treat name_match figures as indicative.',
    job_id            COMMENT 'Lakeflow job id.',
    job_name          COMMENT 'Job display name, e.g. 007_lfj_uc6_ea_flood_warning.',
    run_id            COMMENT 'Unique id of this job run.',
    run_name          COMMENT 'Name recorded for this particular run.',
    run_type          COMMENT 'JOB_RUN for a scheduled/triggered run, SUBMIT_RUN for a one-off submission.',
    result_state      COMMENT 'Terminal outcome: SUCCEEDED, FAILED, CANCELED, TIMEDOUT. NULL while running.',
    is_success        COMMENT 'TRUE when result_state = SUCCEEDED.',
    is_failure        COMMENT 'TRUE when result_state = FAILED.',
    trigger_type      COMMENT 'What started the run (SCHEDULED, ONE_TIME, RETRY, ...).',
    termination_code  COMMENT 'Platform reason code when a run did not succeed -- the first thing to look at for a failure.',
    started_at        COMMENT 'Run start timestamp.',
    ended_at          COMMENT 'Run end timestamp.',
    run_duration_seconds       COMMENT 'Total run duration in seconds as reported by the platform.',
    execution_duration_seconds COMMENT 'Time spent actually executing tasks, excluding setup, queueing and cleanup.',
    queue_duration_seconds     COMMENT 'Time the run spent queued before starting. Persistently high values mean contention, not slow code.',
    setup_duration_seconds     COMMENT 'Time spent provisioning compute before execution.',
    duration_minutes  COMMENT 'Total run duration in minutes, rounded to 2dp. Prefer this for charting.',
    run_date          COMMENT 'Calendar date of the run start, for daily trend aggregation.'
)
COMMENT 'One row per Lakeflow job run, attributed to a FlowX dataflow group where possible, with the platform duration breakdown (queue vs setup vs execution). Check the attribution column before quoting per-group job figures.'
AS
WITH groups AS (
    SELECT dataflow_group_id FROM {control_schema}.dataflow_group_spec
),
job_attr AS (
    SELECT
        j.job_id,
        j.name AS job_name,
        COALESCE(j.tags['dataflow_group_id'], g.dataflow_group_id) AS dataflow_group_id,
        CASE
            WHEN j.tags['dataflow_group_id'] IS NOT NULL THEN 'tag'
            WHEN g.dataflow_group_id IS NOT NULL         THEN 'name_match'
        END AS attribution
    FROM (
        -- One row per job: system.lakeflow.jobs is slowly-changing, so keep only the newest
        -- definition or every run would fan out across historical job versions.
        SELECT job_id, name, tags,
               ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) AS rn
        FROM system.lakeflow.jobs
        WHERE delete_time IS NULL
    ) j
    LEFT JOIN groups g
           ON j.tags['dataflow_group_id'] IS NULL
          AND LOWER(j.name) LIKE CONCAT('%', LOWER(g.dataflow_group_id), '%')
    WHERE j.rn = 1
)
SELECT
    a.dataflow_group_id,
    a.attribution,
    t.job_id,
    a.job_name,
    t.run_id,
    t.run_name,
    t.run_type,
    t.result_state,
    t.result_state = 'SUCCEEDED' AS is_success,
    t.result_state = 'FAILED'    AS is_failure,
    t.trigger_type,
    t.termination_code,
    t.period_start_time AS started_at,
    t.period_end_time   AS ended_at,
    t.run_duration_seconds,
    t.execution_duration_seconds,
    t.queue_duration_seconds,
    t.setup_duration_seconds,
    ROUND(t.run_duration_seconds / 60.0, 2) AS duration_minutes,
    CAST(t.period_start_time AS DATE)       AS run_date
FROM system.lakeflow.job_run_timeline t
LEFT JOIN job_attr a ON t.job_id = a.job_id
"""


def get_cost_view_ddl(observability_schema: str) -> str:
    """DBU and list-price cost attributed to a FlowX dataflow group.

    Cost is reported at list price (``system.billing.list_prices``), which ignores any negotiated
    discount -- the ``estimated_cost_usd`` column name says "estimated" for that reason. Pipeline
    usage attributes exactly, via ``usage_metadata.dlt_pipeline_id``; job usage is attributed only
    when the job carries the ``dataflow_group_id`` tag, since ``custom_tags`` on a usage row
    reflect the compute's tags.
    """
    return f"""
CREATE OR REPLACE VIEW {observability_schema}.v_dataflow_cost (
    dataflow_group_id COMMENT 'FlowX dataflow group the spend is attributed to.',
    usage_date        COMMENT 'Calendar date the usage was recorded against.',
    workload          COMMENT 'Which side of the group incurred the cost: "pipeline" (a Lakeflow pipeline update) or "job" (a Lakeflow job run).',
    entity_name       COMMENT 'Name of the pipeline or job that incurred the cost.',
    sku_name          COMMENT 'Databricks SKU billed, e.g. PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST_OHIO.',
    billing_origin_product COMMENT 'Product family the usage is billed under.',
    dbus              COMMENT 'DBUs consumed. The unit-of-work measure; compare this across groups rather than dollars if pricing differs by region.',
    estimated_cost_usd COMMENT 'DBUs multiplied by the current LIST price for the SKU. An estimate: it does not reflect negotiated discounts, commitments or promotional credits.'
)
COMMENT 'Daily DBU consumption and estimated list-price cost per FlowX dataflow group, split by pipeline vs job workload. Pipeline cost is attributed exactly; job cost requires the dataflow_group_id job tag.'
AS
WITH prices AS (
    -- Current list price per SKU. price_end_time IS NULL selects the row still in effect.
    SELECT sku_name, pricing.effective_list.default AS unit_price
    FROM system.billing.list_prices
    WHERE price_end_time IS NULL
),
pipeline_usage AS (
    SELECT
        r.dataflow_group_id,
        u.usage_date,
        'pipeline'      AS workload,
        r.pipeline_name AS entity_name,
        u.sku_name,
        u.billing_origin_product,
        u.usage_quantity
    FROM system.billing.usage u
    INNER JOIN {observability_schema}.v_pipeline_registry r
            ON u.usage_metadata.dlt_pipeline_id = r.pipeline_id
),
job_usage AS (
    SELECT
        j.tags['dataflow_group_id'] AS dataflow_group_id,
        u.usage_date,
        'job'                       AS workload,
        j.name                      AS entity_name,
        u.sku_name,
        u.billing_origin_product,
        u.usage_quantity
    FROM system.billing.usage u
    INNER JOIN (
        SELECT job_id, name, tags,
               ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) AS rn
        FROM system.lakeflow.jobs
        WHERE delete_time IS NULL AND tags['dataflow_group_id'] IS NOT NULL
    ) j ON u.usage_metadata.job_id = j.job_id AND j.rn = 1
),
combined AS (
    SELECT * FROM pipeline_usage
    UNION ALL
    SELECT * FROM job_usage
)
SELECT
    c.dataflow_group_id,
    c.usage_date,
    c.workload,
    c.entity_name,
    c.sku_name,
    c.billing_origin_product,
    ROUND(SUM(c.usage_quantity), 4)                          AS dbus,
    ROUND(SUM(c.usage_quantity * COALESCE(p.unit_price, 0)), 4) AS estimated_cost_usd
FROM combined c
LEFT JOIN prices p ON c.sku_name = p.sku_name
GROUP BY ALL
"""


def get_flow_metrics_view_ddl(
    observability_schema: str, event_log_tables: List[str]
) -> str:
    """Per-flow row counts and durations, read from the pipelines' Unity Catalog event logs.

    This is the view that makes the dashboard genuinely *data*-observable rather than merely
    operational: ``num_output_rows`` per flow per update is the only place the framework's actual
    throughput is queryable.

    ``event_log_tables`` must be a concrete list of fully-qualified event-log tables, because SQL
    has no way to read a table whose name comes from another table's column -- the names are
    resolved at DDL-build time from ``v_pipeline_registry.event_log_table``. A pipeline that does
    not publish its event log to UC simply contributes no rows.

    When the list is empty the view is still created, over a typed empty relation, so that
    dependent dashboard queries and the Genie space resolve instead of erroring on a missing
    object. ``details`` is a JSON *string* in the event log, hence the ``details:path`` operator.
    """
    if event_log_tables:
        union_sql = "\n    UNION ALL\n".join(
            f"""    SELECT
        origin.pipeline_id   AS pipeline_id,
        origin.update_id     AS update_id,
        origin.flow_name     AS flow_name,
        timestamp            AS event_time,
        details:flow_progress.status                     AS flow_status,
        details:flow_progress.metrics.num_output_rows    AS num_output_rows,
        details:flow_progress.metrics.num_upserted_rows  AS num_upserted_rows,
        details:flow_progress.metrics.num_deleted_rows   AS num_deleted_rows,
        details:flow_progress.data_quality.dropped_records AS dropped_records
    FROM {table}
    WHERE event_type = 'flow_progress'
      AND details:flow_progress.metrics.num_output_rows IS NOT NULL"""
            for table in event_log_tables
        )
    else:
        # Typed empty relation: keeps every dependent object resolvable on a workspace where no
        # pipeline publishes its event log to Unity Catalog yet.
        union_sql = """    SELECT
        CAST(NULL AS STRING)    AS pipeline_id,
        CAST(NULL AS STRING)    AS update_id,
        CAST(NULL AS STRING)    AS flow_name,
        CAST(NULL AS TIMESTAMP) AS event_time,
        CAST(NULL AS STRING)    AS flow_status,
        CAST(NULL AS STRING)    AS num_output_rows,
        CAST(NULL AS STRING)    AS num_upserted_rows,
        CAST(NULL AS STRING)    AS num_deleted_rows,
        CAST(NULL AS STRING)    AS dropped_records
    WHERE FALSE"""

    return f"""
CREATE OR REPLACE VIEW {observability_schema}.v_flow_metrics (
    dataflow_group_id COMMENT 'FlowX dataflow group this flow belongs to.',
    pipeline_name     COMMENT 'Pipeline that executed the flow.',
    pipeline_id       COMMENT 'Lakeflow pipeline UUID.',
    update_id         COMMENT 'Pipeline update in which the flow ran. Join to v_pipeline_updates for the update outcome and duration.',
    flow_name         COMMENT 'Fully-qualified dataset/flow name written by this flow, e.g. flowx.gold.flood_warning_telephone.',
    dataset_name      COMMENT 'Just the table name portion of flow_name, without catalog or schema.',
    flow_status       COMMENT 'Flow status at the time of the event: COMPLETED, RUNNING, FAILED, EXCLUDED.',
    rows_written      COMMENT 'Rows this flow wrote in this update (num_output_rows). The framework throughput measure -- SUM this for volume processed.',
    rows_upserted     COMMENT 'Rows merged/updated, for CDC and SCD flows.',
    rows_deleted      COMMENT 'Rows deleted, for CDC flows handling deletes.',
    rows_dropped      COMMENT 'Rows dropped by data-quality expectations configured to drop failing records.',
    event_time        COMMENT 'Timestamp of the flow-progress event.',
    run_date          COMMENT 'Calendar date of the event, for daily trend aggregation.'
)
COMMENT 'Per-flow, per-update row counts from the Lakeflow event logs, attributed to FlowX dataflow groups. This is the data-throughput view: how many rows each individual flow actually wrote. Only the last flow_progress event per flow per update is kept, so rows_written is not double counted.'
AS
WITH raw_events AS (
{union_sql}
),
ranked AS (
    -- A flow emits many flow_progress events per update (RUNNING ... COMPLETED), each carrying a
    -- cumulative count. Keeping one event per (update, flow) is what stops SUM(rows_written) from
    -- multiplying throughput by the number of progress events.
    --
    -- Prefer a terminal event over the merely newest one. A streaming flow can sit in RUNNING
    -- indefinitely and emit its last progress event mid-batch, so ordering by time alone reports
    -- a partial count and a misleading 'RUNNING' status for an update that in fact finished.
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY pipeline_id, update_id, flow_name
               ORDER BY CASE
                            WHEN flow_status IN ('COMPLETED', 'FAILED', 'EXCLUDED', 'SKIPPED')
                            THEN 0 ELSE 1
                        END,
                        event_time DESC
           ) AS rn
    FROM raw_events
)
SELECT
    r.dataflow_group_id,
    r.pipeline_name,
    e.pipeline_id,
    e.update_id,
    e.flow_name,
    ELEMENT_AT(SPLIT(e.flow_name, '\\\\.'), -1) AS dataset_name,
    e.flow_status,
    CAST(e.num_output_rows   AS BIGINT) AS rows_written,
    CAST(e.num_upserted_rows AS BIGINT) AS rows_upserted,
    CAST(e.num_deleted_rows  AS BIGINT) AS rows_deleted,
    CAST(e.dropped_records   AS BIGINT) AS rows_dropped,
    e.event_time,
    CAST(e.event_time AS DATE)          AS run_date
FROM ranked e
INNER JOIN {observability_schema}.v_pipeline_registry r
        ON e.pipeline_id = r.pipeline_id
WHERE e.rn = 1
"""


def get_dq_results_view_ddl(
    observability_schema: str, event_log_tables: List[str]
) -> str:
    """Per-expectation data-quality outcomes, exploded from the event logs.

    The framework's native DQ signal lives in
    ``details:flow_progress.data_quality.expectations``, a JSON array of
    ``{name, dataset, passed_records, failed_records}``. Exploding it gives one row per rule per
    flow per update -- the grain a customer actually wants to see ("which rule failed, on what,
    how often"), and one the framework does not otherwise persist anywhere.
    """
    expectation_schema = (
        "array<struct<name:string,dataset:string,"
        "passed_records:bigint,failed_records:bigint>>"
    )
    if event_log_tables:
        union_sql = "\n    UNION ALL\n".join(
            f"""    SELECT
        origin.pipeline_id AS pipeline_id,
        origin.update_id   AS update_id,
        origin.flow_name   AS flow_name,
        timestamp          AS event_time,
        details:flow_progress.data_quality.expectations AS expectations_json
    FROM {table}
    WHERE event_type = 'flow_progress'
      AND details:flow_progress.data_quality.expectations IS NOT NULL"""
            for table in event_log_tables
        )
    else:
        union_sql = """    SELECT
        CAST(NULL AS STRING)    AS pipeline_id,
        CAST(NULL AS STRING)    AS update_id,
        CAST(NULL AS STRING)    AS flow_name,
        CAST(NULL AS TIMESTAMP) AS event_time,
        CAST(NULL AS STRING)    AS expectations_json
    WHERE FALSE"""

    return f"""
CREATE OR REPLACE VIEW {observability_schema}.v_dq_results (
    dataflow_group_id COMMENT 'FlowX dataflow group the expectation belongs to.',
    pipeline_name     COMMENT 'Pipeline that evaluated the expectation.',
    update_id         COMMENT 'Pipeline update in which the expectation was evaluated.',
    flow_name         COMMENT 'Fully-qualified flow/dataset the expectation guards.',
    dataset_name      COMMENT 'Table name portion of flow_name.',
    rule_name         COMMENT 'Name of the data-quality expectation as declared in the FlowX dq_config, e.g. uc6_tel_msisdn_max_16.',
    passed_records    COMMENT 'Records that satisfied this expectation in this update.',
    failed_records    COMMENT 'Records that violated this expectation in this update. Non-zero means a real data-quality problem.',
    total_records     COMMENT 'passed_records + failed_records: the records evaluated.',
    pass_rate_pct     COMMENT 'Percentage of evaluated records that passed, 0-100 rounded to 2dp. NULL when no records were evaluated.',
    has_failures      COMMENT 'TRUE when failed_records > 0. Filter on this to list only rules that are actually failing.',
    event_time        COMMENT 'Timestamp of the evaluation.',
    run_date          COMMENT 'Calendar date of the evaluation, for daily trend aggregation.'
)
COMMENT 'One row per data-quality expectation per flow per pipeline update, with pass/fail record counts and pass rate. Sourced from the Lakeflow event logs -- this is the only queryable record of FlowX data-quality rule outcomes.'
AS
WITH raw_events AS (
{union_sql}
),
ranked AS (
    -- As in v_flow_metrics: expectation counts are cumulative within an update, so keep exactly
    -- one event per (update, flow) or every rule's pass/fail totals are multiplied by the number
    -- of progress events the flow happened to emit.
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY pipeline_id, update_id, flow_name
               ORDER BY event_time DESC
           ) AS rn
    FROM raw_events
),
exploded AS (
    SELECT
        e.pipeline_id, e.update_id, e.flow_name, e.event_time,
        exp.name           AS rule_name,
        exp.passed_records AS passed_records,
        exp.failed_records AS failed_records
    FROM ranked e
    LATERAL VIEW EXPLODE(
        FROM_JSON(e.expectations_json, '{expectation_schema}')
    ) t AS exp
    WHERE e.rn = 1
)
SELECT
    r.dataflow_group_id,
    r.pipeline_name,
    x.update_id,
    x.flow_name,
    ELEMENT_AT(SPLIT(x.flow_name, '\\\\.'), -1) AS dataset_name,
    x.rule_name,
    x.passed_records,
    x.failed_records,
    x.passed_records + x.failed_records AS total_records,
    CASE
        WHEN (x.passed_records + x.failed_records) > 0
        THEN ROUND(100.0 * x.passed_records / (x.passed_records + x.failed_records), 2)
    END AS pass_rate_pct,
    x.failed_records > 0 AS has_failures,
    x.event_time,
    CAST(x.event_time AS DATE) AS run_date
FROM exploded x
INNER JOIN {observability_schema}.v_pipeline_registry r
        ON x.pipeline_id = r.pipeline_id
"""


def get_reconciliation_health_view_ddl(observability_schema: str, control_schema: str) -> str:
    """Reconciliation run outcomes joined to their spec, with a derived match rate.

    ``reconciliation_run_log`` is the framework's one genuinely rich runtime table. The caution
    worth encoding: the log is written only when ``logging_config.run_log_capture`` is enabled, so
    *absence of rows is not evidence of a healthy run* -- hence ``has_run_history``.
    """
    return f"""
CREATE OR REPLACE VIEW {observability_schema}.v_reconciliation_health (
    dataflow_group_id  COMMENT 'FlowX dataflow group owning the reconciliation flow.',
    reconciliation_id  COMMENT 'Reconciliation flow identifier.',
    target_id          COMMENT 'The specific target compared against the source in this run.',
    run_id             COMMENT 'Unique id of this reconciliation run.',
    task_run_id        COMMENT 'Job run id, or the pipeline update id in pipeline mode. The correlation key to v_job_runs / v_pipeline_updates. Nullable for standalone runs.',
    status             COMMENT 'SUCCESS, FAILED, or SKIPPED_ALREADY_PROCESSED (the restartability guard found this batch already reconciled).',
    execution_mode     COMMENT 'How the reconciliation runs: batch or pipeline.',
    two_tier_verification COMMENT 'TRUE when the flow performs two-tier verification.',
    source_record_count COMMENT 'Records read from the source side.',
    target_record_count COMMENT 'Records read from the target side.',
    matched_count      COMMENT 'Records present and identical on both sides.',
    missing_in_target_count COMMENT 'Records in the source with no counterpart in the target -- data loss.',
    missing_in_source_count COMMENT 'Records in the target with no counterpart in the source -- unexpected or orphaned data.',
    value_drift_count  COMMENT 'Records present on both sides whose compared column values differ.',
    total_discrepancies COMMENT 'Sum of the three discrepancy counts. The single number to lead with when asked whether a reconciliation is clean.',
    match_rate_pct     COMMENT 'Percentage of source records that matched exactly, 0-100 rounded to 2dp. NULL when the source was empty.',
    is_clean           COMMENT 'TRUE when the run succeeded with zero discrepancies of any kind.',
    has_run_history    COMMENT 'TRUE when this reconciliation has actually logged runs. FALSE means no run has been logged -- which may mean it never ran, OR that logging_config.run_log_capture is disabled. Absence of rows is NOT evidence of health.',
    error_message      COMMENT 'Failure detail when status = FAILED.',
    run_at             COMMENT 'Run timestamp.',
    run_date           COMMENT 'Calendar date of the run, for daily trend aggregation.'
)
COMMENT 'Reconciliation run outcomes joined to their flow spec, with derived match rate and a clean/dirty verdict. Note has_run_history: reconciliation logging is optional, so no rows does not mean no problems.'
AS
SELECT
    s.dataflow_group_id,
    s.reconciliation_id,
    l.target_id,
    l.run_id,
    l.task_run_id,
    l.status,
    s.execution_mode,
    s.two_tier_verification,
    l.source_record_count,
    l.target_record_count,
    l.matched_count,
    l.missing_in_target_count,
    l.missing_in_source_count,
    l.value_drift_count,
    COALESCE(l.missing_in_target_count, 0)
        + COALESCE(l.missing_in_source_count, 0)
        + COALESCE(l.value_drift_count, 0) AS total_discrepancies,
    CASE
        WHEN COALESCE(l.source_record_count, 0) > 0
        THEN ROUND(100.0 * COALESCE(l.matched_count, 0) / l.source_record_count, 2)
    END AS match_rate_pct,
    l.status = 'SUCCESS'
        AND COALESCE(l.missing_in_target_count, 0) = 0
        AND COALESCE(l.missing_in_source_count, 0) = 0
        AND COALESCE(l.value_drift_count, 0) = 0 AS is_clean,
    l.run_id IS NOT NULL AS has_run_history,
    l.error_message,
    l.run_at,
    CAST(l.run_at AS DATE) AS run_date
FROM {control_schema}.reconciliation_flow_spec s
LEFT JOIN {control_schema}.reconciliation_run_log l
       ON s.reconciliation_id = l.reconciliation_id
"""


def get_group_health_summary_view_ddl(observability_schema: str) -> str:
    """The headline scorecard: one row per dataflow group, everything a KPI strip needs.

    Deliberately a *view over the other views* rather than a re-derivation from base tables, so
    that a fix to duration or attribution logic propagates here instead of being reimplemented.
    """
    return f"""
CREATE OR REPLACE VIEW {observability_schema}.v_group_health_summary (
    dataflow_group_id  COMMENT 'FlowX dataflow group identifier.',
    environment        COMMENT 'Environment the group is onboarded for.',
    is_active          COMMENT 'FALSE when the group is soft-disabled.',
    total_flow_count   COMMENT 'Total flows defined across ingestion, transformation and reconciliation.',
    feature_summary    COMMENT 'Human-readable summary of the framework features this group uses. Quote directly when asked what a group does.',
    pipeline_name      COMMENT 'Lakeflow pipeline implementing the group, if one is deployed.',
    total_updates      COMMENT 'Pipeline updates observed in the last 30 days.',
    successful_updates COMMENT 'Updates that completed successfully in the last 30 days.',
    failed_updates     COMMENT 'Updates that failed in the last 30 days.',
    retry_updates      COMMENT 'Updates that were automatic retries of a failure. A high share signals an unstable pipeline.',
    success_rate_pct   COMMENT 'Percentage of terminal updates that succeeded, 0-100. The headline reliability number.',
    avg_duration_minutes COMMENT 'Mean update duration in minutes over the last 30 days, excluding full refreshes.',
    p95_duration_minutes COMMENT ' 95th-percentile update duration in minutes -- the number that reflects user-visible worst case, unlike the mean.',
    last_update_at     COMMENT 'Timestamp of the most recent pipeline update.',
    last_update_state  COMMENT 'Outcome of the most recent update: COMPLETED, FAILED, or NULL if still running.',
    total_rows_written COMMENT 'Rows written across all flows in the last 30 days -- the throughput headline.',
    dq_rules_evaluated COMMENT 'Distinct data-quality rules evaluated in the last 30 days.',
    dq_failed_records  COMMENT 'Records that violated a data-quality expectation in the last 30 days.',
    dq_pass_rate_pct   COMMENT 'Percentage of data-quality evaluations that passed, 0-100.',
    recon_discrepancies COMMENT 'Total reconciliation discrepancies in the last 30 days.',
    estimated_cost_usd_30d COMMENT 'Estimated list-price cost attributed to this group over the last 30 days.',
    dbus_30d           COMMENT 'DBUs consumed by this group over the last 30 days.',
    health_status      COMMENT 'Overall verdict for the group: HEALTHY, DEGRADED, FAILING, INACTIVE, or NO_RUNS. Use this for a status column or traffic-light indicator.'
)
COMMENT 'Headline scorecard: one row per FlowX dataflow group combining reliability, duration, throughput, data quality, reconciliation and cost over the last 30 days, with an overall health verdict. The best single view to answer "how is everything doing".'
AS
WITH upd AS (
    SELECT
        dataflow_group_id,
        ANY_VALUE(pipeline_name) AS pipeline_name,
        COUNT(*)                                                        AS total_updates,
        SUM(CASE WHEN is_success THEN 1 ELSE 0 END)                     AS successful_updates,
        SUM(CASE WHEN is_failure THEN 1 ELSE 0 END)                     AS failed_updates,
        SUM(CASE WHEN is_retry   THEN 1 ELSE 0 END)                     AS retry_updates,
        -- Full refreshes are legitimately far slower; averaging them in makes a healthy pipeline
        -- look like it regressed on whatever day it was rebuilt.
        AVG(CASE WHEN NOT is_full_refresh THEN duration_minutes END)    AS avg_duration_minutes,
        PERCENTILE_APPROX(
            CASE WHEN NOT is_full_refresh THEN duration_minutes END, 0.95)
                                                                        AS p95_duration_minutes,
        MAX(started_at)                                                 AS last_update_at
    FROM {observability_schema}.v_pipeline_updates
    WHERE started_at >= CURRENT_DATE() - INTERVAL 30 DAYS
    GROUP BY dataflow_group_id
),
last_state AS (
    SELECT dataflow_group_id, result_state AS last_update_state
    FROM (
        SELECT dataflow_group_id, result_state,
               ROW_NUMBER() OVER (
                   PARTITION BY dataflow_group_id ORDER BY started_at DESC) AS rn
        FROM {observability_schema}.v_pipeline_updates
    ) WHERE rn = 1
),
flows AS (
    SELECT dataflow_group_id, SUM(rows_written) AS total_rows_written
    FROM {observability_schema}.v_flow_metrics
    WHERE run_date >= CURRENT_DATE() - INTERVAL 30 DAYS
    GROUP BY dataflow_group_id
),
dq AS (
    SELECT
        dataflow_group_id,
        COUNT(DISTINCT rule_name) AS dq_rules_evaluated,
        SUM(failed_records)       AS dq_failed_records,
        CASE
            WHEN SUM(total_records) > 0
            THEN ROUND(100.0 * SUM(passed_records) / SUM(total_records), 2)
        END                       AS dq_pass_rate_pct
    FROM {observability_schema}.v_dq_results
    WHERE run_date >= CURRENT_DATE() - INTERVAL 30 DAYS
    GROUP BY dataflow_group_id
),
recon AS (
    SELECT dataflow_group_id, SUM(total_discrepancies) AS recon_discrepancies
    FROM {observability_schema}.v_reconciliation_health
    WHERE run_date >= CURRENT_DATE() - INTERVAL 30 DAYS
    GROUP BY dataflow_group_id
),
cost AS (
    SELECT
        dataflow_group_id,
        ROUND(SUM(estimated_cost_usd), 2) AS estimated_cost_usd_30d,
        ROUND(SUM(dbus), 2)               AS dbus_30d
    FROM {observability_schema}.v_dataflow_cost
    WHERE usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
    GROUP BY dataflow_group_id
)
SELECT
    c.dataflow_group_id,
    c.environment,
    c.is_active,
    c.total_flow_count,
    c.feature_summary,
    u.pipeline_name,
    COALESCE(u.total_updates, 0)      AS total_updates,
    COALESCE(u.successful_updates, 0) AS successful_updates,
    COALESCE(u.failed_updates, 0)     AS failed_updates,
    COALESCE(u.retry_updates, 0)      AS retry_updates,
    CASE
        WHEN COALESCE(u.successful_updates, 0) + COALESCE(u.failed_updates, 0) > 0
        THEN ROUND(100.0 * u.successful_updates
                   / (u.successful_updates + u.failed_updates), 2)
    END                               AS success_rate_pct,
    ROUND(u.avg_duration_minutes, 2)  AS avg_duration_minutes,
    ROUND(u.p95_duration_minutes, 2)  AS p95_duration_minutes,
    u.last_update_at,
    ls.last_update_state,
    COALESCE(f.total_rows_written, 0) AS total_rows_written,
    COALESCE(dq.dq_rules_evaluated, 0) AS dq_rules_evaluated,
    COALESCE(dq.dq_failed_records, 0) AS dq_failed_records,
    dq.dq_pass_rate_pct,
    COALESCE(rc.recon_discrepancies, 0) AS recon_discrepancies,
    COALESCE(ct.estimated_cost_usd_30d, 0) AS estimated_cost_usd_30d,
    COALESCE(ct.dbus_30d, 0)               AS dbus_30d,
    -- Ordering matters: an inactive group must not be reported as FAILING because of runs that
    -- happened before it was disabled, and a group with no runs at all is a distinct state from
    -- one that is running badly.
    CASE
        WHEN NOT c.is_active                                   THEN 'INACTIVE'
        WHEN COALESCE(u.total_updates, 0) = 0                  THEN 'NO_RUNS'
        WHEN ls.last_update_state = 'FAILED'                   THEN 'FAILING'
        WHEN COALESCE(u.failed_updates, 0) > 0
             OR COALESCE(dq.dq_failed_records, 0) > 0
             OR COALESCE(rc.recon_discrepancies, 0) > 0        THEN 'DEGRADED'
        ELSE 'HEALTHY'
    END AS health_status
FROM {observability_schema}.v_dataflow_group_catalog c
LEFT JOIN upd        u  ON c.dataflow_group_id = u.dataflow_group_id
LEFT JOIN last_state ls ON c.dataflow_group_id = ls.dataflow_group_id
LEFT JOIN flows      f  ON c.dataflow_group_id = f.dataflow_group_id
LEFT JOIN dq         dq ON c.dataflow_group_id = dq.dataflow_group_id
LEFT JOIN recon      rc ON c.dataflow_group_id = rc.dataflow_group_id
LEFT JOIN cost       ct ON c.dataflow_group_id = ct.dataflow_group_id
"""


def get_flow_inventory_view_ddl(observability_schema: str, control_schema: str) -> str:
    """Flat, one-row-per-flow inventory across all three flow kinds.

    The existing dashboard builds this shape inline in several queries; centralising it means the
    Genie space and the documentation generator describe flows identically to the dashboard.
    """
    return f"""
CREATE OR REPLACE VIEW {observability_schema}.v_flow_inventory (
    dataflow_group_id COMMENT 'FlowX dataflow group the flow belongs to.',
    flow_kind         COMMENT 'Which of the three FlowX flow kinds this is: INGESTION, TRANSFORMATION, or RECONCILIATION.',
    flow_id           COMMENT 'Identifier of the flow (dataflow_id, flow_step_id, or reconciliation_id depending on kind).',
    source_description COMMENT 'Where the data comes from: the source connector type and object for ingestion, the declared source inputs for a transformation, the compared source for a reconciliation.',
    target_table      COMMENT 'Fully-qualified target table the flow writes, when it writes one.',
    target_type       COMMENT 'Target materialization type, e.g. streaming_table or materialized_view.',
    cdc_load_strategy COMMENT 'Load strategy, e.g. append, scd_type_1, scd_type_2, truncate_and_load.',
    has_dq            COMMENT 'TRUE when this flow declares data-quality expectations.',
    has_quarantine    COMMENT 'TRUE when failing records are routed to a quarantine table.',
    has_governance_tags COMMENT 'TRUE when Unity Catalog governance tags are applied to the target.',
    is_active         COMMENT 'FALSE when the flow is soft-disabled. NOTE: removing a flow from a spec does NOT set this to FALSE.',
    created_at        COMMENT 'When the flow control row was created.',
    updated_at        COMMENT 'When the flow control row was last updated.'
)
COMMENT 'Flat inventory of every FlowX flow across all three kinds (ingestion, transformation, reconciliation), one row per flow, with its source, target and per-flow feature flags. Use this to answer "what flows are in this group" and "what does this flow do".'
AS
SELECT
    dataflow_group_id,
    'INGESTION' AS flow_kind,
    dataflow_id AS flow_id,
    CONCAT_WS(' ',
        CONCAT('source_type=', source_type),
        CASE WHEN source_system IS NOT NULL
             THEN CONCAT('system=', source_system) END,
        CASE WHEN source_table_name IS NOT NULL
             THEN CONCAT('object=', source_table_name) END) AS source_description,
    CONCAT(target_catalog, '.', target_schema, '.', target_table) AS target_table,
    target_type,
    cdc_load_strategy,
    dq_config_json IS NOT NULL
        AND TRIM(dq_config_json) NOT IN ('', '{{}}')           AS has_dq,
    COALESCE(LOWER(dq_config_json) LIKE '%quarantine%', FALSE) AS has_quarantine,
    governance_tags_json IS NOT NULL
        AND TRIM(governance_tags_json) NOT IN ('', '{{}}')   AS has_governance_tags,
    is_active,
    created_at,
    updated_at
FROM {control_schema}.ingestion_flow_spec

UNION ALL

SELECT
    dataflow_group_id,
    'TRANSFORMATION' AS flow_kind,
    flow_step_id     AS flow_id,
    CONCAT('inputs=', COALESCE(source_inputs_json, '[]'))    AS source_description,
    CONCAT(target_catalog, '.', target_schema, '.', target_table) AS target_table,
    target_type,
    cdc_load_strategy,
    dq_config_json IS NOT NULL
        AND TRIM(dq_config_json) NOT IN ('', '{{}}')           AS has_dq,
    COALESCE(LOWER(dq_config_json) LIKE '%quarantine%', FALSE) AS has_quarantine,
    governance_tags_json IS NOT NULL
        AND TRIM(governance_tags_json) NOT IN ('', '{{}}')   AS has_governance_tags,
    is_active,
    created_at,
    updated_at
FROM {control_schema}.transformation_flow_spec

UNION ALL

SELECT
    dataflow_group_id,
    'RECONCILIATION'  AS flow_kind,
    reconciliation_id AS flow_id,
    CONCAT('compare=', COALESCE(source_config_json, '{{}}')) AS source_description,
    CASE WHEN publish_schema IS NOT NULL
         THEN CONCAT(publish_schema, '.recon__', reconciliation_id) END AS target_table,
    COALESCE(execution_mode, 'batch') AS target_type,
    'reconciliation'                  AS cdc_load_strategy,
    dq_config_json IS NOT NULL
        AND TRIM(dq_config_json) NOT IN ('', '{{}}')  AS has_dq,
    FALSE                             AS has_quarantine,
    FALSE                             AS has_governance_tags,
    is_active,
    created_at,
    updated_at
FROM {control_schema}.reconciliation_flow_spec
"""


def get_lineage_view_ddl(observability_schema: str) -> str:
    """Observed table-to-table lineage for FlowX targets, from ``system.access.table_lineage``.

    Complements the *declared* lineage in ``v_flow_inventory``: this is what actually read and
    wrote what, which is how you catch a flow reading something its spec never mentioned.
    """
    return f"""
CREATE OR REPLACE VIEW {observability_schema}.v_dataflow_lineage (
    dataflow_group_id COMMENT 'FlowX dataflow group whose pipeline produced this lineage edge, when the writing entity is an attributable FlowX pipeline.',
    source_table      COMMENT 'Fully-qualified table that was read. NULL when the read was from a path or an external source rather than a UC table.',
    target_table      COMMENT 'Fully-qualified table that was written.',
    entity_type       COMMENT 'What performed the write: PIPELINE or JOB.',
    entity_name       COMMENT 'Name of the pipeline or job that performed the write.',
    edge_count        COMMENT 'Number of lineage events observed for this source-to-target edge in the window -- an activity measure, not a row count.',
    first_seen        COMMENT 'Earliest observed event for this edge.',
    last_seen         COMMENT 'Most recent observed event for this edge.'
)
COMMENT 'Observed (actual, not declared) table-to-table lineage for tables FlowX pipelines write, from system.access.table_lineage. Use to show real data flow between layers, or to spot reads a spec does not declare.'
AS
SELECT
    r.dataflow_group_id,
    l.source_table_full_name AS source_table,
    l.target_table_full_name AS target_table,
    l.entity_type,
    COALESCE(r.pipeline_name, l.entity_type) AS entity_name,
    COUNT(*)         AS edge_count,
    MIN(l.event_time) AS first_seen,
    MAX(l.event_time) AS last_seen
FROM system.access.table_lineage l
LEFT JOIN {observability_schema}.v_pipeline_registry r
       ON l.entity_id = r.pipeline_id
WHERE l.target_table_full_name IS NOT NULL
GROUP BY ALL
"""


def get_installed_framework_version() -> Optional[str]:
    """The framework wheel version whose code is currently executing, or None if undeterminable.

    This module ships INSIDE the wheel, so the installed distribution version is the version of
    the code asking the question -- no file to read, no bundle variable to thread through. Used as
    the wheel-drift baseline: it is what a redeploy would install right now.
    """
    try:
        from importlib.metadata import version

        return version("flowx")
    except Exception:  # noqa: BLE001 - a source checkout has no installed dist; not an error
        return None


def get_deployment_versions_view_ddl(
    observability_schema: str,
    control_schema: str,
    event_log_tables: List[str],
    installed_version: Optional[str] = None,
) -> str:
    """Which framework wheel each dataflow group last RAN with, and whether that is the newest.

    WHY THIS IS NOT DERIVABLE FROM system.lakeflow. The wheel a pipeline installs lives in its
    ``environment.dependencies`` list, and that field is NOT exposed by
    ``system.lakeflow.pipelines`` -- the ``settings`` struct carries only photon, development,
    continuous, serverless, edition and channel, and ``system.lakeflow.job_tasks`` carries no
    library column at all (both verified against a live workspace).

    WHERE IT COMES FROM INSTEAD. Each pipeline's Unity Catalog event log records the FULL resolved
    pipeline config on every ``create_update`` event, wheel path included, so the version is
    recoverable in pure SQL with no job and no API call. The path shape is
    ``/Volumes/<catalog>/config/wheels/<X.Y.Z>/.internal/flowx-<X.Y.Z>-py3-none-any.whl``
    because ``databricks.yml`` composes ``artifact_path`` from ``wheels_root`` and
    ``framework_version``, so the version is the path segment after ``/wheels/``. A group whose
    pipeline publishes no event log reports NULL rather than being dropped -- silence about a
    group is worse than an explicit unknown.

    THE BASELINE IS THE INSTALLED VERSION, NOT THE BEST OBSERVED ONE. ``installed_version`` is the
    wheel currently deployed (resolved by :func:`get_installed_framework_version`, which reads the
    version of the very distribution this module ships in). Ranking only what has RUN would call
    every group current the moment they all lag together -- the exact state of a workspace whose
    four pipelines all sit on 0.0.3 while the framework ships 0.0.4, where "latest = 0.0.3" is a
    false all-clear. When ``installed_version`` is None the view falls back to the highest observed
    version and says so through ``baseline_source``, so a reader can tell an authoritative
    comparison from a best-effort one. Versions rank by numeric major/minor/patch, because a string
    sort puts 0.0.9 above 0.0.10.

    JOBS ARE JOINED BY TAG, WITH THE ATTRIBUTION STATED. The orchestrating job is matched on the
    ``dataflow_group_id`` job tag. Where that tag is absent the row still appears with
    ``job_attribution = 'none'`` rather than being blended into a tagged figure, because an
    untagged job is a deployment gap to fix, not a missing row.
    """
    if event_log_tables:
        wheel_union = "\n    UNION ALL\n".join(
            f"""    SELECT
        origin.pipeline_id AS pipeline_id,
        timestamp          AS event_time,
        regexp_extract(details, '/wheels/([0-9]+[.][0-9]+[.][0-9]+)/', 1) AS wheel_version,
        regexp_extract(details, '(flowx-[0-9.]+-py3-none-any[.]whl)', 1)  AS wheel_file
    FROM {table}
    WHERE event_type = 'create_update'"""
            for table in event_log_tables
        )
    else:
        wheel_union = """    SELECT
        CAST(NULL AS STRING)    AS pipeline_id,
        CAST(NULL AS TIMESTAMP) AS event_time,
        CAST(NULL AS STRING)    AS wheel_version,
        CAST(NULL AS STRING)    AS wheel_file
    WHERE FALSE"""

    installed_sql = (
        "'" + installed_version.replace("'", "''") + "'"
        if installed_version
        else "CAST(NULL AS STRING)"
    )

    return f"""
CREATE OR REPLACE VIEW {observability_schema}.v_deployment_versions (
    dataflow_group_id  COMMENT 'FlowX dataflow group.',
    environment        COMMENT 'Environment the group is registered for.',
    pipeline_name      COMMENT 'Lakeflow pipeline implementing the group. NULL when no pipeline carries this group id in its configuration.',
    pipeline_id        COMMENT 'Lakeflow pipeline UUID.',
    total_flow_count   COMMENT 'Flows the group declares, from the control tables.',
    wheel_version      COMMENT 'Framework wheel version this group was last observed RUNNING, parsed from the wheel path in its pipeline event log. NULL when the pipeline publishes no event log to Unity Catalog, or has never run.',
    wheel_file         COMMENT 'Wheel filename observed, e.g. flowx-0.0.4-py3-none-any.whl.',
    latest_wheel_version COMMENT 'The wheel version this group is compared against: the INSTALLED framework version when it could be resolved, else the highest version observed running. Read baseline_source to tell which.',
    baseline_source    COMMENT 'installed when latest_wheel_version is the deployed framework version (authoritative), best_observed when it is only the highest version seen running (a fallback -- every group can look current while all of them lag).',
    is_on_latest_wheel COMMENT 'TRUE when this group ran the newest observed wheel. FALSE means the group runs older framework code and should be redeployed. NULL when unknown -- treat NULL as unverified, never as up to date.',
    wheel_status       COMMENT 'Plain-language drift verdict for display: on latest, BEHIND (needs redeploy), or unknown (no event log). Use this column in dashboards rather than re-deriving the wording.',
    versions_behind    COMMENT 'How many distinct observed wheel versions sit between this group and the newest. 0 when current, NULL when unknown.',
    last_run_wheel_at  COMMENT 'When the wheel version was last observed for this group, i.e. the most recent update start.',
    job_name           COMMENT 'Job that orchestrates this group, matched on the dataflow_group_id job tag. NULL when no job carries the tag.',
    job_id             COMMENT 'Numeric job id of the orchestrating job.',
    job_attribution    COMMENT 'How the job was matched: tag when the job carries a dataflow_group_id tag, none when no tagged job exists. A none row is a deployment gap -- the tag is declared in resources/uc*/**_job.yml and only reaches the workspace on redeploy.',
    job_is_bundle_managed COMMENT 'TRUE when the job was deployed by a Declarative Automation Bundle. FALSE flags a hand-created job that no deploy keeps in sync.',
    job_paused         COMMENT 'TRUE when the job schedule is paused.',
    spec_version       COMMENT 'Spec version recorded at onboarding. This is the SPEC version, unrelated to the wheel version.',
    last_onboarded_at  COMMENT 'When the group was last onboarded.'
)
COMMENT 'Framework wheel drift per dataflow group: which wheel each group last ran, whether that is the newest observed, and which job orchestrates it. Answers "which flow is on which wheel and which is not on the latest" without leaving the dashboard. The wheel version is parsed from each pipeline event log because system.lakeflow does not expose pipeline library dependencies.'
AS
WITH wheel_events AS (
{wheel_union}
),
wheel_per_pipeline AS (
    -- Most recent create_update that actually named a wheel. An event whose config carried no
    -- dependency yields an empty match, which must not outrank a real one.
    SELECT
        pipeline_id,
        wheel_version,
        wheel_file,
        event_time,
        ROW_NUMBER() OVER (PARTITION BY pipeline_id ORDER BY event_time DESC) AS rn
    FROM wheel_events
    WHERE wheel_version IS NOT NULL AND wheel_version <> ''
),
observed AS (
    -- Rank observed versions numerically: string ordering puts 0.0.9 above 0.0.10.
    SELECT
        wheel_version,
        DENSE_RANK() OVER (
            ORDER BY
                CAST(split(wheel_version, '[.]')[0] AS INT) DESC,
                CAST(split(wheel_version, '[.]')[1] AS INT) DESC,
                CAST(split(wheel_version, '[.]')[2] AS INT) DESC
        ) AS version_rank
    FROM (SELECT DISTINCT wheel_version FROM wheel_per_pipeline WHERE rn = 1)
),
best_observed AS (
    SELECT wheel_version FROM observed WHERE version_rank = 1 LIMIT 1
),
newest AS (
    -- The installed wheel wins when known; otherwise fall back to the best observed, flagged.
    SELECT
        COALESCE({installed_sql}, (SELECT wheel_version FROM best_observed))
            AS latest_wheel_version,
        CASE WHEN {installed_sql} IS NULL THEN 'best_observed' ELSE 'installed' END
            AS baseline_source
),
tagged_jobs AS (
    SELECT
        j.tags['dataflow_group_id'] AS tag_group_id,
        j.name                      AS job_name,
        j.job_id,
        j.deployment.kind = 'BUNDLE' AS job_is_bundle_managed,
        j.paused,
        j.change_time,
        ROW_NUMBER() OVER (
            PARTITION BY j.tags['dataflow_group_id'] ORDER BY j.change_time DESC) AS rn
    FROM system.lakeflow.jobs j
    WHERE j.delete_time IS NULL
      AND j.tags['dataflow_group_id'] IS NOT NULL
)
SELECT
    g.dataflow_group_id,
    g.environment,
    r.pipeline_name,
    r.pipeline_id,
    g.total_flow_count,
    w.wheel_version,
    w.wheel_file,
    n.latest_wheel_version,
    n.baseline_source,
    CASE WHEN w.wheel_version IS NULL THEN CAST(NULL AS BOOLEAN)
         ELSE w.wheel_version = n.latest_wheel_version END AS is_on_latest_wheel,
    CASE WHEN w.wheel_version IS NULL
              THEN 'unknown - no event log'
         WHEN w.wheel_version = n.latest_wheel_version
              THEN CONCAT('on latest (', w.wheel_version, ')')
         ELSE CONCAT('BEHIND - running ', w.wheel_version,
                     ', latest is ', n.latest_wheel_version)
    END AS wheel_status,
    CASE WHEN w.wheel_version IS NULL THEN CAST(NULL AS INT)
         WHEN w.wheel_version = n.latest_wheel_version THEN 0
         ELSE CAST(
             (SELECT COUNT(DISTINCT v.wheel_version)
                FROM (SELECT wheel_version FROM observed
                      UNION
                      SELECT n.latest_wheel_version AS wheel_version) v
               WHERE CAST(split(v.wheel_version,'[.]')[0] AS INT) * 1000000
                   + CAST(split(v.wheel_version,'[.]')[1] AS INT) * 1000
                   + CAST(split(v.wheel_version,'[.]')[2] AS INT)
                   > CAST(split(w.wheel_version,'[.]')[0] AS INT) * 1000000
                   + CAST(split(w.wheel_version,'[.]')[1] AS INT) * 1000
                   + CAST(split(w.wheel_version,'[.]')[2] AS INT)) AS INT)
    END AS versions_behind,
    w.event_time AS last_run_wheel_at,
    t.job_name,
    t.job_id,
    CASE WHEN t.job_name IS NOT NULL THEN 'tag' ELSE 'none' END AS job_attribution,
    t.job_is_bundle_managed,
    t.paused AS job_paused,
    g.spec_version,
    g.last_onboarded_at
FROM {observability_schema}.v_dataflow_group_catalog g
LEFT JOIN {observability_schema}.v_pipeline_registry r
       ON r.dataflow_group_id = g.dataflow_group_id
LEFT JOIN wheel_per_pipeline w
       ON w.pipeline_id = r.pipeline_id AND w.rn = 1
LEFT JOIN observed o
       ON o.wheel_version = w.wheel_version
LEFT JOIN tagged_jobs t
       ON t.tag_group_id = g.dataflow_group_id AND t.rn = 1
CROSS JOIN newest n
"""


def get_all_observability_view_ddls(
    observability_schema: str,
    control_schema: str,
    event_log_tables: List[str],
) -> List[Tuple[str, str]]:
    """Return ``(description, ddl)`` for every observability view, in dependency order.

    Order matters: ``v_pipeline_registry`` must exist before the views that join to it, and
    ``v_group_health_summary`` is built on top of five of the others, so it is created last.

    ``event_log_tables`` is the list of fully-qualified Unity Catalog event-log tables to read
    per-flow metrics and DQ results from. Resolve it with
    ``notebooks/01_setup/01_setup_control_tables.py``'s query against ``v_pipeline_registry``;
    pass ``[]`` on a workspace where no pipeline publishes its event log to UC and the two
    event-log-backed views will be created empty but resolvable.
    """
    return [
        ("create view v_dataflow_group_catalog",
         get_dataflow_group_catalog_view_ddl(observability_schema, control_schema)),
        ("create view v_flow_inventory",
         get_flow_inventory_view_ddl(observability_schema, control_schema)),
        ("create view v_pipeline_registry",
         get_pipeline_registry_view_ddl(observability_schema, control_schema)),
        ("create view v_pipeline_updates",
         get_pipeline_updates_view_ddl(observability_schema)),
        ("create view v_job_runs",
         get_job_runs_view_ddl(observability_schema, control_schema)),
        ("create view v_dataflow_cost",
         get_cost_view_ddl(observability_schema)),
        ("create view v_flow_metrics",
         get_flow_metrics_view_ddl(observability_schema, event_log_tables)),
        ("create view v_dq_results",
         get_dq_results_view_ddl(observability_schema, event_log_tables)),
        ("create view v_reconciliation_health",
         get_reconciliation_health_view_ddl(observability_schema, control_schema)),
        ("create view v_dataflow_lineage",
         get_lineage_view_ddl(observability_schema)),
        ("create view v_group_health_summary",
         get_group_health_summary_view_ddl(observability_schema)),
        ("create view v_deployment_versions",
         get_deployment_versions_view_ddl(
             observability_schema, control_schema, event_log_tables,
             get_installed_framework_version())),
    ]
