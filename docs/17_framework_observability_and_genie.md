# 🔭 FlowX — Framework Observability, AI/BI & Genie

> **Audience**: Platform engineers who maintain the framework's own observability surface, SREs
> who answer "how is everything doing", FinOps analysts attributing DBU spend to a dataflow
> group, and anyone pointing an LLM at FlowX metadata.

> **Companion page**: [`08_observability_and_telemetry.md`](08_observability_and_telemetry.md) is
> the **export-out** half — OTLP payloads, Volume archives and driver logs. This page is the
> **store-and-query** half. See §1.1 for why both exist.

---

## 1. Why this exists

FlowX has measured itself accurately since v1.0. It has never been able to *answer questions
about* itself.

Every runtime number the framework computes — per-flow `num_output_rows`, per-expectation
pass/fail counts, DQ quarantine counts, SCD upsert and delete counts — is computed and then
**shipped out**: as OTel `ResourceLogs` to an OTLP collector, as JSONL/GZIP files to a Unity
Catalog Volume, or as structured JSON into the driver log. That is the right architecture for
alerting and for a customer's existing observability estate. It is the wrong architecture for a
question, because none of it lands in a queryable Delta table. Asking "which DQ rule failed most
often last week" meant reading Volume files with Spark; asking "what did `dfg_uc6_ea_flood_warning`
cost" meant nothing at all, because cost was never in the export in the first place.

Meanwhile the platform has been recording the operational half all along, in tables nobody was
joining to:

| System table | What it knows |
|---|---|
| `system.lakeflow.pipelines` | Every pipeline, its settings, and its `configuration` MAP |
| `system.lakeflow.pipeline_update_timeline` | Every update: start, end, result state, trigger type |
| `system.lakeflow.jobs` / `job_run_timeline` | Every job and job run, with the queue/setup/execution duration split |
| `system.billing.usage` / `list_prices` | DBUs per pipeline id and job id; current list price per SKU |
| `system.access.table_lineage` | Which entity read what and wrote what, observed rather than declared |
| `<catalog>.<schema>.event_log_<pipeline_id>` | Per-flow row counts and per-expectation DQ results, when published to UC |

**Neither half is useful alone.** The system tables know a pipeline ran for 94 seconds; they do
not know it is `dfg_uc6_ea_flood_warning`, implementing 11 flows, with 5 DQ rules and two
reconciliations. The control tables know exactly that, and know nothing about the 94 seconds.

This feature set is the join, expressed once, as a semantic layer.

### 1.1 The division of labour with doc 08

The two are complementary, not overlapping, and the distinction is worth keeping straight because
they answer different questions from different data:

| | Doc 08 — telemetry export | Doc 17 — observability semantic layer |
|---|---|---|
| Direction | Out of the platform | Stays in the platform |
| Mechanism | `observability[]` destinations; OTLP HTTP, Volume files | Views in `<catalog>.observability` over system tables |
| Grain | Per event, per update | Per group, per update, per flow, per rule, per day |
| Consumers | Dynatrace / Datadog / Splunk / an OTel collector | AI/BI dashboard, Genie space, documentation job, ad-hoc SQL |
| Configuration | Onboarding spec (`observability[]`) | None — provisioned by `01_setup`, no spec attribute |
| Cost data | Absent | First-class (`v_dataflow_cost`) |
| Answers | "Page me when a pipeline fails" | "What did this group cost, and is its DQ passing" |

Nothing in this page replaces doc 08. A customer running Dynatrace still wants the OTLP export;
they *also* want to be able to ask a question in SQL.

### 1.2 What was built

| Asset | Path |
|---|---|
| View DDL builders (pure strings) | `src/flowx/lakeflow_framework/control_plane/observability_views.py` |
| Provisioning (section 5, non-fatal) | `notebooks/01_setup/01_setup_control_tables.py` |
| AI/BI dashboard | `databricks-bi/flowx_observability_dashboard.lvdash.json` + `resources/flowx_bi/flowx_observability_dashboard.yml` |
| Genie space | `databricks-genie/flowx_observability.geniespace.json` + `resources/flowx_genie/flowx_observability_genie_space.yml` |
| Documentation renderer (pure functions) | `src/flowx/lakeflow_framework/observability/dataflow_documenter.py` |
| Documentation job + notebook + Volume | `resources/flowx_docs/*.yml`, `notebooks/09_documentation/09_dataflow_documentation.py` |

**No onboarding spec attribute was added or removed.** `onboarding/spec_validator.py`,
`onboarding_templates/onboarding_spec.schema.json` and the Spec Builder app's registry are
deliberately untouched: nothing here is configured through a spec. The join key was already being
written by every pipeline resource (§3), so there was nothing new for an author to declare. That
also means there is no removal to reject, and no attribute delta document for this release.

---

## 2. Architecture: one semantic layer, three consumers

```
┌────────────────────────────┐        ┌──────────────────────────────────────┐
│  FlowX CONTROL TABLES      │        │  DATABRICKS SYSTEM TABLES            │
│  <catalog>.config          │        │  system.lakeflow.pipelines           │
│  • dataflow_group_spec     │        │  system.lakeflow.pipeline_update_    │
│  • ingestion_flow_spec     │        │      timeline                        │
│  • transformation_flow_spec│        │  system.lakeflow.jobs /              │
│  • reconciliation_flow_spec│        │      job_run_timeline                │
│  • reconciliation_run_log  │        │  system.billing.usage / list_prices  │
│  • onboarding_audit_log    │        │  system.access.table_lineage         │
└─────────────┬──────────────┘        └──────────────────┬───────────────────┘
              │                                          │
              │   configuration['dataflow.group.id']     │
              │   tags['dataflow_group_id']              │
              └──────────────────┬───────────────────────┘
                                 ▼
            ┌────────────────────────────────────────────────┐
            │   <catalog>.observability   — 11 VIEWS         │
            │   commented for humans AND for an LLM          │
            └───────┬───────────────┬──────────────┬─────────┘
                    ▼               ▼              ▼
        ┌───────────────────┐ ┌───────────┐ ┌─────────────────────┐
        │  AI/BI dashboard  │ │  Genie    │ │ Documentation job   │
        │  9 pages, 103     │ │  space    │ │ one Markdown design │
        │  widgets          │ │  (Q&A)    │ │ doc per group       │
        └───────────────────┘ └───────────┘ └─────────────────────┘
              ↑ plus per-pipeline UC EVENT LOG tables, for per-flow rows and DQ results
```

### 2.1 Why views, and why in their own schema

**Why views rather than materialized tables.** The system tables are already materialized by the
platform, on the platform's own refresh cadence. Copying them into FlowX-owned tables would add a
refresh job to keep green, a storage cost, and a staleness window — in exchange for nothing, since
these queries are dashboard-scale, not scan-the-lakehouse scale. A view is also cheap to fix: a
corrected `has_cdc` predicate is one `CREATE OR REPLACE VIEW` away, with no backfill.

**Why a separate `<catalog>.observability` schema rather than `<catalog>.config`.** This is a
grant boundary. The control tables carry `*_json` and `raw_spec_payload` columns holding connection
strings and credential-shaped configuration. A BI user or a Genie space needs the *derived* facts,
not those blobs. Putting the views in their own schema means `GRANT SELECT ON SCHEMA
<catalog>.observability` gives a consumer the whole observability surface **without** granting read
access to the control tables. Had the views lived in `config`, every dashboard viewer would have
needed schema-level read on the secrets-adjacent columns too.

**Why the SQL lives in a Python module and not in the dashboard.** Two consumers need identical
semantics: the AI/BI dashboard and the Genie space. A Genie space answers well only when pointed at
a small set of well-named, well-commented objects rather than at raw system tables — and a
dashboard whose SQL is copy-pasted into a Genie space drifts from it inside one release. Defining
the join once, as commented views, gives both a single source of truth, and gives the documentation
job the same facts for free. Consistent with `control_plane/ddl_definitions.py`, the module is
**pure string building**: no `spark.sql` call happens in it. `01_setup_control_tables.py` is the
only executor.

**Column `COMMENT`s are load-bearing, not decoration.** Genie reads them as its semantic model.
They are written to be read by an LLM: they say what a column means, what to join on, what to
`SUM`, and — importantly — what *not* to conclude (see `has_run_history` and `attribution`).

> [!WARNING]
> **The brace trap applies here.** As in `ddl_definitions.py`, any literal `{` or `}` inside these
> f-string DDL bodies — including inside a column `COMMENT` and including the JSON-empty-object
> literal `'{}'` — **must** be escaped as `{{ }}`. An unescaped brace is evaluated as a replacement
> field and breaks *every* statement in the module, not just the one it appears in. This module
> compares `TRIM(dq_config_json) NOT IN ('', '{{}}')` in four places for exactly this reason.

---

## 3. The join key

### 3.1 Pipelines: free, because the configuration block already carries it

`system.lakeflow.pipelines.configuration` is a `MAP<STRING, STRING>`, and **every FlowX pipeline
resource already sets `dataflow.group.id` in its `configuration:` block** — it has to, because that
is how a pipeline finds its own control rows at graph-definition time (see doc 08 §6.1). So:

```sql
p.configuration['dataflow.group.id'] AS dataflow_group_id
```

recovers the FlowX identity of any pipeline **with no change to any pipeline, resource file, or
spec**. That single expression is the spine of the whole layer: `v_pipeline_registry` computes it
once and every run, cost, metric, DQ and lineage view joins through that view rather than
re-deriving it.

`v_pipeline_registry` also filters `configuration['dataflow.group.id'] IS NOT NULL`, which is what
keeps non-FlowX pipelines in the workspace out of the layer entirely, and `delete_time IS NULL`,
which drops deleted pipelines.

### 3.2 Jobs: not free, and why a tag was needed

The job side looked like it should work the same way. It does not.

The framework passes `dataflow_group_id` to its notebook tasks as a **`base_parameter`**. Those do
**not** surface in `system.lakeflow.job_task_run_timeline.task_parameters` — verified empty on a
live workspace, not inferred from documentation. There is therefore no platform-surfaced,
per-run value carrying the FlowX identity of a job run.

Two mechanisms, in priority order, and a third state that is deliberately *not* silence:

| Priority | Mechanism | `attribution` | Quality |
|---|---|---|---|
| 1 | `system.lakeflow.jobs.tags['dataflow_group_id']` | `'tag'` | **Exact.** Set declaratively on the job resource. |
| 2 | Case-insensitive containment of a known group id in the job name | `'name_match'` | **Heuristic.** Correct by convention, not by contract. |
| 3 | Neither matched | `NULL` group, `NULL` attribution | Row is still returned. |

A `dataflow_group_id` job tag was therefore added to `resources/{uc3,uc6,uc7}/*_job.yml`.

**Why `attribution` is a column and not a filter.** Blending an exact attribution with a heuristic
one and reporting a single number is how a dashboard becomes untrustworthy: the reader cannot tell
which figures are guaranteed. And dropping unattributable runs would hide every framework job that
predates the tag — a *silent* undercount, which is worse than a visible one. So all three states
are returned, and the reader is told which they are looking at. `v_dataflow_cost` takes the
stricter line for money: **job cost is attributed only by tag**, never by name match, because a
name-matched dollar figure invites arithmetic nobody should do.

> [!IMPORTANT]
> **The tag only takes effect for runs that happen after the tagged jobs are redeployed.**
> `system.lakeflow.jobs` records the job definition as it was; historical runs of an untagged job
> stay on the `name_match` path forever. Job-side cost attribution therefore has a start date, not
> a backfill.

### 3.3 Event log location is discovered, not constructed

When a pipeline publishes its event log to Unity Catalog, the table is named
`event_log_<pipeline_id with hyphens replaced by underscores>`. The catalog and schema, however,
are **not recoverable from `system.lakeflow.pipelines`**: the `settings` struct exposes only
`photon`, `development`, `continuous`, `serverless`, `edition` and `channel`. There is no `catalog`
and no `target` field. (This was found the hard way — see §8, defect 5.)

So `v_pipeline_registry` matches the id-derived name against
`system.information_schema.tables WHERE table_name LIKE 'event_log_%'`, taking whatever catalog and
schema the table was actually published to and leaving `event_log_table` **NULL** for a pipeline
that publishes no event log. A `ROW_NUMBER()` on `(table_catalog, table_schema)` keeps the join
one-to-one if the same name somehow exists twice.

---

## 4. The 11 views

Creation order is a dependency order, returned by
`get_all_observability_view_ddls(observability_schema, control_schema, event_log_tables)`:
`v_pipeline_registry` must exist before anything that joins to it, and `v_group_health_summary` is
built on five of the others, so it is created last.

| # | View | Grain | Reads |
|---|---|---|---|
| 1 | `v_dataflow_group_catalog` | one row per dataflow group | control tables only |
| 2 | `v_flow_inventory` | one row per flow (all 3 kinds) | control tables only |
| 3 | `v_pipeline_registry` | one row per FlowX pipeline | `system.lakeflow.pipelines` + `information_schema` |
| 4 | `v_pipeline_updates` | one row per pipeline update | `pipeline_update_timeline` ⋈ (3) |
| 5 | `v_job_runs` | one row per job run | `jobs`, `job_run_timeline` + control |
| 6 | `v_dataflow_cost` | group × date × workload × SKU | `system.billing.*` ⋈ (3) |
| 7 | `v_flow_metrics` | update × flow | UC event logs ⋈ (3) |
| 8 | `v_dq_results` | update × flow × expectation | UC event logs ⋈ (3) |
| 9 | `v_reconciliation_health` | one row per recon run | `reconciliation_flow_spec` ⟕ `reconciliation_run_log` |
| 10 | `v_dataflow_lineage` | source→target edge | `system.access.table_lineage` ⟕ (3) |
| 11 | `v_group_health_summary` | one row per dataflow group | views 1, 4, 6, 7, 8, 9 |

### 4.1 `v_dataflow_group_catalog` — the dimension

One row per dataflow group: what it contains, which framework features it exercises, and who
onboarded it. This is the view that answers *"what is this dataflow group and what does it do"* —
the single most common question asked of the Genie space.

| Column | Purpose |
|---|---|
| `dataflow_group_id` | The primary business key for the whole schema. Join on this. |
| `environment`, `catalog_name`, `is_active` | Onboarded environment, target catalog, soft-disable flag |
| `ingestion_flow_count`, `transformation_flow_count`, `reconciliation_flow_count`, `total_flow_count` | Composition, and "how big is this group" |
| `source_types`, `target_types`, `cdc_load_strategies`, `target_tables` | Distinct values, comma-separated, sorted |
| `has_cdc`, `has_dq`, `has_quarantine`, `has_reconciliation`, `has_governance_tags` | Pre-computed feature flags |
| `feature_summary` | A human-readable sentence describing the group's capability footprint |
| `onboarded_by`, `last_onboarded_at`, `spec_version` | From the most recent **SUCCESS** row of `onboarding_audit_log` |
| `created_at`, `updated_at` | Control-row lifecycle |

Two design decisions worth knowing:

- **The feature flags are pre-computed rather than left to the consumer.** `has_dq` is a predicate
  over `dq_config_json`; `has_cdc` is a pattern match over the load strategies. Leaving an LLM (or
  a dashboard author) to infer them from raw JSON blobs is how you get five different, mutually
  inconsistent definitions of "this group uses CDC".
- **`feature_summary` exists because Genie reproduces a sentence far more reliably than it composes
  one.** It is a `CONCAT_WS(' ', ...)` of conditional clauses — flow counts, ingest types, load
  strategies, and one clause each for DQ, quarantine, reconciliation, governance tags and
  inactivity. Its column comment tells Genie to quote it directly.
- **`onboarded_by`/`spec_version` come from a `ROW_NUMBER()`, not a `MAX() GROUP BY`.** They must
  come from the *same* audit row as the timestamp; `MAX()` on each column independently would
  happily combine one person's identity with another's spec version.

> [!NOTE]
> `is_active = FALSE` means the group is soft-disabled and the engine skips it. **Deleting a flow
> from a spec does not set this to `FALSE`** — the removed flow's control row stays active and keeps
> driving the DAG. Both `v_dataflow_group_catalog.is_active` and `v_flow_inventory.is_active` carry
> that warning in their column comments, because it is the single most surprising fact about the
> control plane.

### 4.2 `v_flow_inventory` — flat, across all three flow kinds

A `UNION ALL` of the three flow-spec tables into one shape: `dataflow_group_id`, `flow_kind`
(`INGESTION` / `TRANSFORMATION` / `RECONCILIATION`), `flow_id`, `source_description`,
`target_table`, `target_type`, `cdc_load_strategy`, the three per-flow feature flags, `is_active`,
and the two timestamps.

`flow_id` is whichever identifier the kind uses (`dataflow_id`, `flow_step_id`,
`reconciliation_id`), and `source_description` is built per kind: `source_type=/system=/object=`
for ingestion, `inputs=<source_inputs_json>` for a transformation, `compare=<source_config_json>`
for a reconciliation. Reconciliation rows carry the synthetic `cdc_load_strategy` value
`'reconciliation'` and `target_type` = the execution mode, since neither concept applies.

It exists because the dashboard was building this shape inline in several queries. Centralising it
means the Genie space and the documentation generator describe a flow **identically** to the
dashboard.

### 4.3 `v_pipeline_registry` — the bridge

Covered in §3. Beyond the join key it surfaces `pipeline_name`, `pipeline_type`, `control_catalog`
(from `configuration['dataflow.control.catalog']`), the five settings booleans/enums
(`is_serverless`, `is_continuous`, `is_development`, `is_photon`, `edition`, `channel`), `run_as`,
`created_by`, the discovered `event_log_table`, and `create_time`/`change_time`.

### 4.4 `v_pipeline_updates` — run performance

One row per update. Notable derivations:

- **`duration_seconds` / `duration_minutes` are derived from the period bounds**, because
  `pipeline_update_timeline` carries **no duration column**.
- **`is_retry` surfaces `trigger_type = 'RETRY_ON_FAILURE'` explicitly.** A burst of retry rows is
  the signature of a pipeline failing and auto-retrying; on a plain run-count chart it reads as
  healthy activity.
- **`is_full_refresh`** exists so a reader can exclude full refreshes before comparing durations. A
  full refresh is legitimately far slower, and averaging it in makes a healthy pipeline look like
  it regressed on whatever day it was rebuilt.
- `result_state` is `NULL` while an update is still running, so `is_success`/`is_failure` are the
  columns to do arithmetic on.

### 4.5 `v_job_runs` — with an attribution-quality column

One row per job run, with the platform's full duration breakdown: `run_duration_seconds`,
`execution_duration_seconds`, `queue_duration_seconds`, `setup_duration_seconds`. The split
matters diagnostically — persistently high `queue_duration_seconds` is contention, not slow code,
and high `setup_duration_seconds` is compute provisioning.

`system.lakeflow.jobs` is slowly-changing, so the view keeps only the newest non-deleted definition
per `job_id` via `ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC)`. Without that,
every run fans out across every historical version of its job. `attribution` is §3.2.

### 4.6 `v_dataflow_cost` — DBUs and estimated list-price cost

Grain is `dataflow_group_id × usage_date × workload × entity × sku`.

- **`workload`** is `'pipeline'` or `'job'`, so a reader can see which side of a group spends.
- **Pipeline usage attributes exactly**, via `usage_metadata.dlt_pipeline_id` joined to
  `v_pipeline_registry`.
- **Job usage requires the tag** (§3.2).
- **`estimated_cost_usd` is `dbus × current list price`**, from
  `system.billing.list_prices WHERE price_end_time IS NULL`. The column is named "estimated" for a
  reason: list price ignores negotiated discounts, commitments and promotional credits. Its comment
  says so, and so does the dashboard.
- **Compare `dbus`, not dollars, across groups** when regions differ — the column comment says
  this too, because it is the mistake an LLM makes most readily.

### 4.7 `v_flow_metrics` — the throughput view

Per-flow, per-update row counts from the pipelines' Unity Catalog event logs:
`rows_written` (`num_output_rows`), `rows_upserted`, `rows_deleted`, `rows_dropped`, plus
`flow_status`, `flow_name`, `dataset_name`, `event_time` and `run_date`.

This is the view that makes the layer genuinely **data**-observable rather than merely
operational. `num_output_rows` per flow per update is the only place the framework's actual
throughput is queryable at all.

**Why `event_log_tables` is a build-time argument.** SQL cannot read a table whose name comes from
another table's column. The event-log table names are therefore resolved at DDL-build time — by
`01_setup` querying `v_pipeline_registry.event_log_table` — and baked into a `UNION ALL` over the
concrete list. A pipeline that publishes no event log simply contributes no rows.

**When the list is empty the view is still created**, over a typed empty relation
(`SELECT CAST(NULL AS ...) ... WHERE FALSE`). This is deliberate: dependent dashboard datasets, the
Genie space and `v_group_health_summary` all resolve against a real object with the right column
types, instead of erroring on a missing table. An empty chart is a correct answer; a broken
dashboard is not.

**One event per (update, flow), and the terminal one.** A flow emits many `flow_progress` events
per update, each carrying a **cumulative** count, so keeping them all multiplies throughput by the
number of progress events. Deduplication is a `ROW_NUMBER()` — but ordering by `event_time DESC`
alone is *wrong*, and was a shipped bug (§8, defect 4). Terminal statuses are ordered first:

```sql
ORDER BY CASE WHEN flow_status IN ('COMPLETED','FAILED','EXCLUDED','SKIPPED') THEN 0 ELSE 1 END,
         event_time DESC
```

`details` is a JSON **string** in the event log, hence the `details:flow_progress.metrics.*`
path operator throughout.

### 4.8 `v_dq_results` — per-expectation outcomes

`details:flow_progress.data_quality.expectations` is a JSON array of
`{name, dataset, passed_records, failed_records}`. `FROM_JSON` + `LATERAL VIEW EXPLODE` gives one
row per **rule per flow per update** — the grain a customer actually wants ("which rule failed, on
what, how often"), and one the framework does not otherwise persist anywhere.

Derived columns: `total_records` (passed + failed), `pass_rate_pct` (`NULL` when nothing was
evaluated, never a spurious 0 or 100), and `has_failures` to filter to rules that are actually
failing. `rule_name` is the expectation name as declared in the FlowX `dq_config`, so a dashboard
row points straight back at a spec line.

The same cumulative-count deduplication as `v_flow_metrics` applies, for the same reason.

### 4.9 `v_reconciliation_health` — and `has_run_history`

`reconciliation_flow_spec` **LEFT JOIN** `reconciliation_run_log`, so a reconciliation that has
never run still appears. Derived: `total_discrepancies` (missing-in-target + missing-in-source +
value-drift), `match_rate_pct`, and `is_clean` (`SUCCESS` **and** all three discrepancy counts
zero).

> [!WARNING]
> **`has_run_history` is the important column.** `reconciliation_run_log` is written only when
> `logging_config.run_log_capture` is enabled. So **absence of rows is not evidence of a healthy
> reconciliation** — it may mean the flow never ran, or it may mean logging is switched off.
> `has_run_history = FALSE` says "I cannot tell you", which is a different and more honest answer
> than an empty discrepancy count. The column comment spells this out, because an LLM asked "are
> the reconciliations clean?" will otherwise read zero rows as zero problems.

`status` also carries `SKIPPED_ALREADY_PROCESSED`, the restartability guard finding a batch already
reconciled — a success, not a failure, and not a run.

### 4.10 `v_dataflow_lineage` — observed, not declared

`system.access.table_lineage`, grouped to one row per `(source_table, target_table, entity)` edge
with `edge_count`, `first_seen` and `last_seen`. It complements the **declared** lineage in
`v_flow_inventory`: this is what actually read and wrote what, which is how you catch a flow
reading something its spec never mentions.

`source_table` is `NULL` when the read was from a path or an external source rather than a UC
table — that is normal for a Bronze ingestion node, not a defect. `edge_count` is an **activity**
measure, not a row count.

### 4.11 `v_group_health_summary` — the scorecard

One row per dataflow group, everything a KPI strip needs, over a **rolling 30-day** window:
reliability (`total_updates`, `successful_updates`, `failed_updates`, `retry_updates`,
`success_rate_pct`), duration (`avg_duration_minutes`, `p95_duration_minutes`, both excluding full
refreshes), recency (`last_update_at`, `last_update_state`), throughput (`total_rows_written`),
quality (`dq_rules_evaluated`, `dq_failed_records`, `dq_pass_rate_pct`), integrity
(`recon_discrepancies`), spend (`estimated_cost_usd_30d`, `dbus_30d`), and the verdict.

**It is a view over the other views, not a re-derivation from base tables.** A fix to duration
arithmetic or attribution logic propagates here instead of being reimplemented — and diverging.

`p95_duration_minutes` sits next to the mean because the mean is not the number a user
experiences.

**`health_status` — and why the `CASE` order is the design:**

| Verdict | Condition |
|---|---|
| `INACTIVE` | `NOT is_active` |
| `NO_RUNS` | no updates in the window |
| `FAILING` | the **most recent** update failed |
| `DEGRADED` | any failed update, any DQ failed record, or any recon discrepancy in the window |
| `HEALTHY` | none of the above |

The order matters. `INACTIVE` is checked first so a group disabled *after* a bad run is not reported
as `FAILING` for runs that happened before it was switched off. `NO_RUNS` is checked before
`FAILING` because "nothing ran" is a distinct state from "it ran badly", and conflating them makes
a never-deployed group look broken. `FAILING` outranks `DEGRADED` because the latest state is what
an operator acts on.

---

## 5. The AI/BI dashboard

`databricks-bi/flowx_observability_dashboard.lvdash.json`, deployed by
`resources/flowx_bi/flowx_observability_dashboard.yml`: **13 datasets, 9 pages, 103 widgets,
15 charts.**

| Page | Datasets | What it answers |
|---|---|---|
| **Overview** | `group_scorecard`, `pipeline_updates`, `flow_throughput`, `flow_inventory` | 8 counters (groups, flows, rows, 30d cost, updates, success rate, DQ pass rate, recon discrepancies) plus updates-per-day-by-outcome, rows-per-day-by-group, and the two at-a-glance tables. |
| **Pipeline Performance** | `pipeline_updates` | Update volume vs average duration (combo), duration by group, what triggers updates, and every update in a table. Retries are counted separately from throughput. |
| **Data Flow & Throughput** | `flow_throughput` | Rows written / upserted / dropped-by-DQ; rows by dataset; daily throughput area by group; per-flow per-update detail. |
| **Quality & Reconciliation** | `dq_results`, `recon_health` | Passed vs failed per rule, a dataset × rule **heatmap** of failed records, reconciliation classification per flow, and the recon run table. |
| **Cost & Efficiency** | `cost_daily`, `group_scorecard` | Daily cost area by group, share-of-spend pie, cost-vs-rows combo (the efficiency question), and the daily usage records. |
| **Framework & Lineage** | `flow_inventory`, `lineage` | Flows per group by kind, load strategies in use, a **sankey** of source→target flow, and the full flow inventory. |
| **AI Forecast** | `forecast_spend`, `forecast_runs`, `forecast_summary` | Two `forecast-line` charts (hourly spend, hourly updates) plus 4 counters including **Hours of History**. See §6 before trusting a number here. |
| **Job Orchestration** | `job_runs` | Job success rate, average queue time, run time split by phase, and every run with its `attribution`. |
| **Global Filters** | `group_filter` + 9 others | A multi-select **Dataflow Group** bound across 10 datasets, a **Date range** picker across the 6 time-series datasets, and an **Environment** single-select. |

> [!NOTE]
> **The three forecast datasets are deliberately not filter-bound.** A forecast fitted to a
> filtered subset of hours is a forecast of a different series than the one the chart claims to
> show, and `AI_FORECAST` will not tell you that. The AI Forecast page is workspace-wide by
> construction.

### 5.1 `dataset_schema: observability`, and the two-part-name subtlety

The resource sets `dataset_schema: observability` — **not** `config`, which is what the existing
`flowx_control_dashboard.yml` uses.

The mechanism to understand: `dataset_catalog` / `dataset_schema` **only fill in what a query
omits.** A bare `v_group_health_summary` resolves to
`<dataset_catalog>.observability.v_group_health_summary`; a query that spells out more of the path
keeps what it spelled. That gives three tiers, and this dashboard uses two of them:

| Name form in the query | Resolves to | Used by |
|---|---|---|
| Bare — `v_group_health_summary` | `<dataset_catalog>.observability.<name>` | All 10 view-backed datasets |
| Three-part — `system.billing.usage` | Itself, unchanged | The 2 forecast datasets that need raw hourly usage |
| Two-part — `config.<table>` | `<dataset_catalog>.config.<table>` | Nothing, **currently** |

The two-part form is the escape hatch for reaching a **control table** from a dashboard whose
`dataset_schema` is `observability` — it overrides only the schema, leaving the catalog to be
injected per target. As shipped, no dataset needs it: `v_dataflow_group_catalog` already surfaces
the onboarding-audit facts a widget would otherwise have gone to `config.onboarding_audit_log` for.
Use the two-part form if you add one; two unit tests
(`test_no_dataset_query_hardcodes_a_catalog_or_the_control_schema` and
`test_dataset_queries_only_read_known_observability_objects`) will reject a hard-coded catalog.

This is the same pattern documented for `flowx_control_dashboard` (dashboard queries keep bare
table names; `dataset_catalog`/`dataset_schema` inject them per target), applied with two schemas
in play instead of one.

---

## 6. AI_FORECAST — the four rules, and the ten-billion-DBU cautionary tale

The **AI Forecast** page uses `forecast-line` widgets over Databricks' `AI_FORECAST` table-valued
function. It works well. It is also the one part of this feature set that will produce a
*confidently absurd* number if the four rules below are not followed, so they are encoded in the
dashboard SQL, in the Genie space instructions, and here.

### 6.1 The cautionary tale

The first implementation forecast a **daily** series. On a young workspace only **4 days** of
billing history existed, so `AI_FORECAST` was fitting a seasonal model to **3 usable daily points**
— and projected:

> **10,257,538,803 DBUs by day 14.**

Ten billion DBUs. The function did not error. It did not warn. It returned a well-formed number,
with a confidence band, in a chart that looked exactly like a working forecast. That is the failure
mode to internalise: **an under-fed forecast fails plausibly, not loudly.**

The fix is grain. `date_trunc('HOUR', ...)` over the same 14-day window gives **73 points** instead
of 3, and a credible forecast.

### 6.2 The four rules

| # | Rule | Why |
|---|---|---|
| **1** | **Hourly grain, never daily.** `date_trunc('HOUR', usage_start_time)` / `date_trunc('HOUR', started_at)`. | A daily series on a young workspace has single-digit points. `AI_FORECAST` needs enough observations to fit anything; hourly buys ~24× the history from the same calendar window. |
| **2** | **Exclude the current, partly-elapsed bucket.** `WHERE usage_start_time < date_trunc('HOUR', current_timestamp())`. | The in-flight hour is 5 minutes of usage sitting in a 60-minute bucket. Left in, it is the *most recent* point and reads as a collapse in spend — so the model extrapolates a decline that is purely an artefact of when you looked. |
| **3** | **Always pass `parameters => '{"global_floor": 0}'`.** | Without a floor, the lower confidence band goes **negative** — negative DBUs, negative dollars. Cost and run counts are non-negative by construction; the model does not know that unless told. |
| **4** | **Report the band and the history depth, not just the point forecast.** | A point forecast with no interval and no "fitted on N hours" is indistinguishable from a measurement. The `Forecast Summary` dataset returns `observed_hours`, `forecast_next_48h_low_usd` and `forecast_next_48h_high_usd` alongside the point value precisely so a widget cannot show the number without the caveat. |

And the framing rule that goes with them: **say it is list price, and say it is a projection, not a
commitment.** `estimated_cost_usd` already carries the list-price caveat (§4.6); a forecast of it
inherits that *plus* model uncertainty.

### 6.3 How the dashboard SQL implements them

Every forecast dataset has the same four-part shape:

```sql
WITH actuals AS (   -- hourly (rule 1), current bucket excluded (rule 2)
  SELECT date_trunc('HOUR', s.usage_start_time) AS usage_hour, ... AS cost_usd
  FROM scoped s LEFT JOIN prices p ON s.sku_name = p.sku_name
  WHERE s.usage_start_time < date_trunc('HOUR', current_timestamp())
  GROUP BY 1
),
bounds AS (SELECT MAX(usage_hour) AS max_h FROM actuals),
forecast AS (
  SELECT usage_hour, cost_usd_forecast, cost_usd_upper, cost_usd_lower, CAST(NULL AS DOUBLE) AS cost_usd
  FROM AI_FORECAST(
    TABLE(actuals),
    horizon    => (SELECT max_h + MAKE_DT_INTERVAL(0, 48, 0, 0) FROM bounds),   -- +48h
    time_col   => 'usage_hour',
    value_col  => 'cost_usd',
    parameters => '{"global_floor": 0}'                                          -- rule 3
  )
),
bridge AS (...)     -- the last actual point, duplicated into the forecast columns
SELECT ... FROM actuals UNION ALL SELECT ... FROM bridge UNION ALL SELECT ... FROM forecast
ORDER BY usage_hour
```

Three details worth keeping when you edit this:

- **`horizon` is an absolute timestamp**, computed as `MAX(actual hour) + MAKE_DT_INTERVAL(0,48,0,0)`
  — anchored to the last *observed* point, not to `current_timestamp()`, so a gap in billing
  ingestion shortens the horizon rather than silently forecasting across the gap.
- **The `bridge` CTE** emits the final actual point *again* with its value copied into
  `cost_usd_forecast` / `_upper` / `_lower`. Without it the actual line and the forecast line do not
  touch, and the chart shows a one-hour hole at the join.
- **Actuals and forecast are `UNION ALL`ed into one column set**, each side `NULL`-padding the
  other's columns. That is what lets a single `forecast-line` widget draw history and projection
  together.

The `Forecast Summary` dataset is rule 4 in SQL: `observed_hours`, `observed_cost_usd`,
`observed_dbus`, `avg_hourly_cost_usd`, `forecast_next_48h_usd` with its low/high band,
`projected_30d_run_rate_usd` (mean forecast hour × 24 × 30), and
`forecast_vs_actual_delta_frac` — the last of these being the honest self-check: how far the
forecast's mean hour sits from the observed mean hour.

### 6.4 Live verification

Against `metaflow_v7` at the time of writing, with ~30 hours of billing history:

| Figure | Value |
|---|---|
| Observed cost (14-day window, hourly) | **$23.19** |
| Forecast, next 48 hours | **$18.94** |
| Projected 30-day run rate | **$290.08** |

Sane, mutually consistent, and — with 30 hours of history — still wide. Which is the point of rule 4.

---

## 7. The Genie space

`databricks-genie/flowx_observability.geniespace.json`, deployed by
`resources/flowx_genie/flowx_observability_genie_space.yml`. It answers questions like:

- *"Show me details of every dataflow group and what features they use"*
- *"Explain / document dataflow group X"*
- *"Forecast our spend"*

**15 data sources, 1 instruction block, 12 example question/SQL pairs, 12 sample questions,
4 benchmarks.**

### 7.1 Why the views, and only two raw system tables

The fifteen data sources are: the **11 observability views**, **2 control tables**
(`config.onboarding_audit_log` and `config.reconciliation_mismatch_log` — the row-level detail the
"who onboarded this" and "what exactly did not match" questions need), and **2 raw system tables**
(`system.billing.usage`, `system.billing.list_prices`) — exposed *only* because `AI_FORECAST` needs
the raw hourly usage grain that `v_dataflow_cost` has already aggregated to a day.

> [!NOTE]
> Unlike the dashboard, a Genie space's `data_sources.tables` carry **fully-qualified three-part
> identifiers** with the catalog spelled literally (`flowx.observability.v_...`). There is no
> `dataset_catalog` equivalent to inject it. Deploying this space to a differently-named catalog
> means editing those identifiers — and keeping them sorted (§7.2 rule 3).

That restraint is the whole design. A Genie space pointed at raw `system.lakeflow.*` has to
rediscover the `configuration['dataflow.group.id']` join, the slowly-changing-jobs
deduplication, the cumulative-event-count trap and the terminal-status ordering **on every
question**, and will get at least one of them wrong. Pointed at eleven views whose column comments
state the join keys and the caveats, it mostly just reads the answer off.

### 7.2 The serialized format — three hard requirements

The format was reverse-engineered from `databricks bundle generate genie-space`, and it is
stricter than it looks. All three of these are **API-enforced**, not stylistic:

1. **`version: 2`** at the top level.
2. **Every human-readable string is an ARRAY OF LINES**, each element ending in `\n` **except the
   last**. A plain string where the schema wants a line array is rejected.
3. **Repeated blocks must be sorted.** `data_sources.tables` must be sorted by **identifier**, and
   every other repeated block must be sorted by **`id`**. The API *hard-rejects* an unsorted array
   — it does not normalise it for you. This is the one that bites when you hand-add a table or an
   example: append it and the deploy fails; insert it in sort position and it succeeds.

### 7.3 The round-trip workflow

Genie spaces are edited in the UI in practice — someone adds an instruction, fixes an example's
SQL, tunes a benchmark. Those edits must come **back into the repo**, or the next deploy silently
reverts them:

```bash
databricks bundle generate genie-space \
  --resource flowx_observability_genie_space \
  --force
```

`--force` overwrites the local JSON with the workspace's current definition, re-sorted and
re-serialized in the canonical format. Run it after any UI edit, review the diff, commit. Going the
other way — repo to workspace — is an ordinary `bundle deploy`.

> [!NOTE]
> Hand-editing the JSON is fine, and sometimes necessary (a new view means a new data source). Just
> respect §7.2's sorting, and prefer a `bundle generate` round-trip afterwards to normalise
> whatever you wrote.

---

## 8. The documentation generator

Genie answers conversationally. Sometimes you need an artefact you can commit, attach to a ticket,
or hand to a customer. That is what this is: **the batch counterpart to the Genie space**, reading
the same views so the two cannot disagree about the facts.

| Asset | Role |
|---|---|
| `src/flowx/lakeflow_framework/observability/dataflow_documenter.py` | Pure functions: plain Python data in, Markdown string out. No Spark, no I/O. |
| `notebooks/09_documentation/09_dataflow_documentation.py` | The only place that reads the views and writes files. |
| `resources/flowx_docs/dataflow_documentation_job.yml` | The job that runs it. |
| `resources/flowx_docs/framework_docs_volume.yml` | The Volume it writes into. |

Output: one Markdown design document per dataflow group, into
`/Volumes/<catalog>/config/framework_docs/dataflow_groups/`, plus a `README.md` index. The job takes
four parameters:

| Parameter | Default | Meaning |
|---|---|---|
| `catalog` | `${var.catalog}` | Which catalog's `observability` schema to read |
| `dataflow_group_id` | `""` | A single group, or blank for **all** groups |
| `output_volume` | `""` | Blank derives `/Volumes/<catalog>/config/framework_docs/dataflow_groups` |
| `lookback_days` | `"30"` | The reporting window for the operational sections |

Files are written with `dbutils.fs.put(..., overwrite=True)` — Volume paths are not writable
through plain Python file IO on serverless compute.

**The degrade-not-fail contract.** Only a missing or empty `v_dataflow_group_catalog` is fatal (a
`RuntimeError`: with no groups there is nothing to document). `v_flow_inventory` is likewise read
directly. Everything else — `v_group_health_summary`, `v_dq_results`, `v_reconciliation_health`,
`v_dataflow_lineage` — goes through an `optional_rows()` helper that logs a warning and returns
`[]`. So on a workspace with no system-table grant the job still produces the **declarative**
half of every document (composition, flows, features, targets) and simply omits the operational
sections, rather than producing nothing. `dbutils.fs.mkdirs` on the output path is itself wrapped,
because the Volume may already exist.

**Eight sections per group**, ordered the way a reader builds a mental model rather than
alphabetically:

1. Purpose and scale
2. Framework capabilities exercised
3. Flows — ingestion, then transformation, then reconciliation, each kind prefaced with what that
   kind *is*
4. Data quality
5. Reconciliation
6. Operational behaviour
7. Observed lineage
8. Tables this group publishes

Two decisions worth recording:

- **It reads the control tables, not the spec files.** So the document describes the system **as
  onboarded and as running** — not as some spec file on a laptop claims it should be. When the two
  disagree, the document is right and the spec is stale, which is exactly the direction of trust
  you want from generated documentation.
- **The pure-function boundary is a testability decision.** There is no local Spark in this repo
  (databricks-connect plus no Java), so anything that touches a session is untestable offline.
  Keeping rendering in plain functions is what makes 31 unit tests possible without a cluster.

---

## 9. Prerequisites, and exactly what breaks when they are unmet

Section 5 of `notebooks/01_setup/01_setup_control_tables.py` provisions the schema and the views. It
is **deliberately non-fatal**: system-table access is a workspace grant the deploying principal may
not hold, and observability is not a prerequisite for onboarding a pipeline. A failure logs a
warning and setup continues.

There are **two nested guards**, because the two failures are different:

- **Inner** — event-log discovery fails. `event_log_tables` becomes `[]` and the two
  event-log-backed views are created **empty but resolvable** (§4.7). Logged as: *"Could not
  discover pipeline event log tables (…); creating the event-log-backed views empty but
  resolvable."*
- **Outer** — the schema DDL or any view DDL fails. Nothing is re-raised, so onboarding never
  fails on this step. Logged as: *"Observability semantic layer was NOT fully provisioned in
  '<schema>': … The control tables are unaffected. Most often this means the deploying principal
  lacks SELECT on the system catalog — grant it, then re-run this notebook."*

The discovery query is the concrete form of §3.3, joining candidate event-log table names back to
live FlowX-tagged pipelines so it never picks up an unrelated `event_log_*` table:

```sql
SELECT DISTINCT concat(t.table_catalog,'.',t.table_schema,'.',t.table_name) AS event_log_table
FROM system.information_schema.tables t
JOIN system.lakeflow.pipelines p
  ON replace(t.table_name, 'event_log_', '') = replace(p.pipeline_id, '-', '_')
WHERE t.table_name LIKE 'event_log_%'
  AND p.delete_time IS NULL
  AND p.configuration['dataflow.group.id'] IS NOT NULL
```

| Prerequisite | Needed for | What happens without it |
|---|---|---|
| `SELECT` on `system.lakeflow.*`, `system.billing.*`, `system.access.table_lineage` | Every view except `v_dataflow_group_catalog` and `v_flow_inventory` | Section 5 logs a **warning**; onboarding still succeeds. Views may be created but **error at query time**, so **the dashboard's widgets error** while the objects exist. |
| A pipeline publishing its **event log to Unity Catalog** | `v_flow_metrics`, `v_dq_results` | Views exist over a typed **empty** relation. Per-flow metrics and DQ results are **empty, not broken** — every dependent widget renders, showing nothing. `v_group_health_summary` reports 0 rows written and 0 DQ rules. |
| `dataflow_group_id` **job tag**, deployed | `v_job_runs.attribution = 'tag'`, all job-side cost | Runs fall back to `name_match` (visible in the column); job **cost is not attributed at all** (cost is tag-only, §3.2). Historical runs never gain the tag. |
| `logging_config.run_log_capture` enabled | `v_reconciliation_health` run rows | `has_run_history = FALSE`. **Do not read the zero discrepancy count as clean** (§4.9). |
| `dataflow.group.id` in the pipeline `configuration:` block | Everything | The pipeline is invisible to the whole layer — `v_pipeline_registry` filters it out. Every FlowX pipeline resource already sets it. |

**The one asymmetry to remember:** a missing *grant* leaves you with views that exist and fail;
a missing *event log* leaves you with views that exist and are empty. The first is a red widget,
the second is a blank one, and they need different fixes.

### 9.1 Re-provisioning

The view DDL is `CREATE OR REPLACE VIEW` throughout, and the schema DDL is
`CREATE SCHEMA IF NOT EXISTS`. Re-running `01_setup` is therefore idempotent and is also how you
pick up a corrected view definition — **and how you pick up newly-published event logs**, since
`event_log_tables` is resolved at DDL-build time. Onboard a new group, run its pipeline once so it
publishes an event log, then re-run `01_setup` for its rows to appear in `v_flow_metrics`.

---

## 10. Gotchas — six bugs live data caught

Every one of these passed code review, produced **no error**, and returned a **plausible wrong
answer**. That is the theme worth carrying away: a SQL semantic layer over metadata fails quietly,
and the only thing that catches it is running it against real data and reading the output
critically. All six are fixed; they are recorded because the *class* of each will recur.

### 10.1 `has_cdc` was always `FALSE` — case

The control tables store load strategies **UPPERCASE** (`SCD1`, `SCD2`, `APPEND`,
`TRUNCATE_AND_LOAD`). The predicate was a lowercase-only `RLIKE`. Every SCD group reported
`has_cdc = false`, on a dashboard tile whose whole job was to say which groups do CDC.

```sql
LOWER(cdc_load_strategies) RLIKE '(scd|cdc|merge|truncate)' AS has_cdc
```

**Lesson:** these control-table enum values are **not case-normalised**. Fold the case explicitly
in every predicate over them — do not assume the case you happened to see in one spec.

### 10.2 A leading `", "` on every aggregated list — `SPLIT('')` is not empty

Merging the ingestion and transformation lists of target types looked simple:
`SPLIT(COALESCE(a,''), ', ')` on each side, `FLATTEN`, `ARRAY_DISTINCT`, `CONCAT_WS`. But when one
side has no rows, `COALESCE` yields `''` — and **`SPLIT('', ', ')` returns `['']`: an array with
**one EMPTY element**, not an empty array.** That element sorts first and `CONCAT_WS` renders it as
a leading `", "`.

**`ARRAY_COMPACT` does not fix it** — it removes `NULL`s, not empty strings. The fix is an explicit
filter:

```sql
FILTER(FLATTEN(ARRAY(SPLIT(...), SPLIT(...))), x -> x IS NOT NULL AND x <> '')
```

**Lesson:** `SPLIT` of an empty string is a one-element array. Anywhere you `SPLIT` a
possibly-empty aggregate, filter for non-empty rather than reaching for `ARRAY_COMPACT`.

### 10.3 `has_dq` was `TRUE` for every group — `'{}'` is a non-empty string

`dq_config_json` is literally the two characters `'{}'` on many onboarded flows — an *empty* DQ
config, faithfully serialized. Testing `dq_config_json IS NOT NULL AND dq_config_json <> ''`
reported "this group has data quality" for every group in the workspace.

```sql
dq_config_json IS NOT NULL AND TRIM(dq_config_json) NOT IN ('', '{{}}')
```

**Lesson:** for a JSON-in-a-string column, "present" and "non-empty" are different questions.
Note the `{{}}` escaping — this predicate appears four times in the module and every one of them is
a brace trap.

### 10.4 `flow_status = RUNNING` with a partial row count — newest ≠ terminal

Deduplicating `flow_progress` events by `event_time DESC` alone looked obviously right. It is not:
a streaming flow can sit in `RUNNING` and emit its last progress event **mid-batch**, so the newest
event for a finished update carries a partial cumulative count and a `RUNNING` status. The
dashboard showed completed updates as running, with understated throughput.

The fix orders terminal statuses first, *then* by time (§4.7).

**Lesson:** in an event log, the last event you received is not the same thing as the event that
concluded the thing. Order by state, then by time.

### 10.5 `FIELD_NOT_FOUND` on `settings.catalog`

`v_pipeline_registry` originally constructed the event-log table name as
`<settings.catalog>.<settings.target>.event_log_<id>`. `system.lakeflow.pipelines.settings` has no
`catalog` and no `target` field — only `photon`, `development`, `continuous`, `serverless`,
`edition`, `channel`. This one *did* error, loudly, at view-creation time, which is why it is the
least expensive of the six. The fix is discovery via `system.information_schema.tables` (§3.3).

**Lesson:** verify a system table's actual schema before deriving anything from it. A struct's
contents are not guessable from what the equivalent API response contains.

### 10.6 The repo's brace trap, again

`'{}'` inside an f-string DDL body must be written `'{{}}'`. This is the same trap
`ddl_definitions.py` documents for column `COMMENT`s, and it now has a second home. An unescaped
brace does not break one statement — Python fails to evaluate the f-string, so **every** DDL in the
module breaks at import.

**Lesson:** `python -c "import ..."` on the module is the cheapest possible check, and it catches
this instantly.

---

## 11. Tests

| Suite | Tests | Covers |
|---|---|---|
| `tests/unit/test_observability_views_ddl.py` | 29 fns / 33 cases | Every view's DDL builds; the brace escaping survives; column-comment presence; the empty-`event_log_tables` typed-empty-relation path; the six defects above pinned as regressions. |
| `tests/unit/test_dataflow_documenter.py` | 28 fns / 31 cases | Section ordering and presence; Markdown table escaping (a `|` in a value); `None`/`''`/`'{}'` collapsing to a dash; every `health_status` blurb. |
| `tests/unit/test_observability_dashboard_and_genie.py` | 30 fns / 32 cases | Dashboard/Genie JSON well-formedness; the four AI_FORECAST rules asserted in the SQL; Genie sort-order and line-array format requirements; `dataset_schema` and two-part-name conventions. |

**96 new tests, all passing.** The pre-existing offline baseline is confirmed unchanged at
**12 failed, 1262 passed, 2 skipped, 117 errors** — the 117 errors being the known
no-local-Spark/Java condition, and the 12 failures pre-dating this work
(`test_config_validation_negative_spec` 3, `test_optional_fields_df_customer_ingest_spec` 5,
`test_recon_backward_compatibility` 1, `test_source_plane_plan` 3).

`resources/flowx_genie/` and `resources/flowx_docs/` are registered in **both** `databricks.yml`'s
`include:` list and `tests/unit/test_resource_layout.py`'s `EXPECTED_GROUPS` — that test enforces
the pairing, so a new resource group cannot be added to one without the other.

---

**See also:**
[`08_observability_and_telemetry.md`](08_observability_and_telemetry.md) for the export-out
telemetry engines (OTLP, Volume, driver logs) — the complement to this page;
[`04_data_quality_and_governance.md`](04_data_quality_and_governance.md) for how the expectations
`v_dq_results` reports are declared;
[`07_reconciliation_engine.md`](07_reconciliation_engine.md) for `logging_config.run_log_capture`,
which decides whether `v_reconciliation_health` has anything to show;
[`13_known_limitations_and_gotchas.md`](13_known_limitations_and_gotchas.md) for the wider gotcha
catalogue.
