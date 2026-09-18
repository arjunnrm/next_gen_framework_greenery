# :material-table-cog: Control metadata dashboard · page by page

**The `Metaflow Control Metadata` dashboard shows the system as onboarded: every flow row in the control tables, its load strategy and configuration JSON, the audit trail of who onboarded what, and which framework wheel each group runs on.** It reads the `config` schema only, so it works before a single pipeline has run and needs no system-table grants. Source: `databricks-bi/flowx_control_metadata_dashboard.lvdash.json`, deployed by `resources/flowx_bi/flowx_control_dashboard.yml` with `dataset_schema: config`.

!!! abstract "Quick links"
    - The tables behind it: [Platform architecture §4, the control-plane ERD](../01_platform_architecture.md#4-control-plane-metadata-schema-erd)
    - What writes them: [Spec Builder · run onboarding](spec_builder.md#run-onboarding-from-the-app) and the `onboarding_job`
    - The runtime twin: [Observability dashboard](observability_dashboard.md)

## Datasets

| Dataset | Reads | Used by |
|---|---|---|
| Dataflow Groups Filter | `dataflow_group_spec` | Global Filters |
| Main Audit & Flow Table, KPI Metrics Filtered | `ingestion_flow_spec` ∪ `transformation_flow_spec` ∪ `reconciliation_flow_spec` (+ `onboarding_audit_log`) | Overview |
| Ingestion Flows Detail, Transformation Flows Detail, Reconciliation Detail | the matching `*_flow_spec` table (Reconciliation also joins `reconciliation_run_log`) | one page each |
| Audit Log Detail | `onboarding_audit_log` | Observability & Audit |
| Raw Spec JSON (debug) | the `*_config_json` columns | Raw JSON |
| Deployment Health (wheel drift) | `v_deployment_versions` over `dataflow_group_spec` and the pipeline registry | Overview |

## Global Filters

One filter, **Dataflow Group ID**, bound to every page. Leave it empty for the estate view.

## Overview

<div class="fx-mock" markdown="0">┌ Total Flows ┐┌ Active Groups ┐┌ Groups Behind ┐┌ Target Wheel ┐
│     41      ││       7       ││       1       ││  flowx-0.0.7 │
└─────────────┘└───────────────┘└───────────────┘└──────────────┘
│ Which group runs which framework wheel (table)                       │
│ Flow inventory: group · flow · kind · source/target · strategy · active│</div>

- **Widgets:** Total Flows · Active Groups · Groups Behind · Target Wheel · which group runs which framework wheel · flow inventory.
- **Reading it:** **Groups Behind** counts groups whose pipeline resolves an older wheel than the target's declared `framework_version`. The wheel directory is per version (`/Volumes/<catalog>/config/wheels/<X.Y.Z>`), so "behind" means the pipeline has not been updated since the last deploy.

**Action checklist**

- [ ] Groups Behind above zero after a deploy: run each lagging pipeline once so it resolves the new wheel.
- [ ] Never deploy while a pipeline update is running; a mid-update deploy prunes the wheel that update is installing.

## Ingestion Flows

- **Widgets:** Total Ingestion Flows · Active Ingestion Flows · Unique Source Types · Target Tables · Ingestion Flow Details.
- **Columns in the detail table:** `dataflow_id`, `dataflow_group_id`, `source_type`, `target_catalog.target_schema.target_table`, `target_type`, `cdc_load_strategy`, `is_active`, `updated_at`, plus the `source_config_json` and `target_config_json` documents.
- **Use it to:** confirm that a spec edit actually landed (compare `updated_at` with your onboarding run), and to find every flow reading a given `source_type` before a framework change.

**Action checklist**

- [ ] A flow you deleted from the spec is still `is_active = true`: onboarding does not deactivate removed flows unless you pass `prune_missing_flows=true`; it keeps driving the DAG until you do.
- [ ] Attribute-level detail: open **Raw JSON** for the flow, or the [Ingestion attribute reference](../reference/json/ingestion.md).

## Transformation Flows

- **Widgets:** Total Transformation Flows · Active Transformations · Unique Target Types · CDC Strategies · Transformation Flow Details.
- **Columns:** `flow_step_id`, `dataflow_id`, `dataflow_group_id`, target three-part name, `target_type`, `cdc_load_strategy`, `is_active`, `updated_at`, `source_inputs_json`, `transformation_sql`, `target_config_json`.
- **Use it to:** audit which SCD strategies are in use per group and read the exact SQL the pipeline runs (after `${param}` substitution happens at update time, not here).

## Reconciliation

- **Widgets:** Total Reconciliation Flows · Total Runs · Total Matched Records · Total Drift Detected · Reconciliation Details.
- **Columns:** `reconciliation_id`, `dataflow_group_id`, `execution_mode`, `publish_schema`, `two_tier_verification`, `match_keys_json`, `compare_columns_json`, and the run-log roll-up (`matched_count`, `value_drift_count`, `missing_in_target_count`, `missing_in_source_count`, last `status`).
- **Reading it:** counts come from `reconciliation_run_log`, which is written only when `logging_config.run_log_capture` is true (the onboarded default since v1.7.3 is **false** in pipeline mode unless `publish_schema` is set). A flow with runs but no counts has logging switched off, not zero drift.

**Action checklist**

- [ ] Drift above zero: row-level detail is in `config.reconciliation_mismatch_log`; the Observability dashboard's [Quality & Reconciliation](observability_dashboard.md#quality-reconciliation) page trends it.
- [ ] Heal it: see [Pillar 3 · self-healing append](../pillars/reconciliation.md#self-healing-append).

## Observability & Audit

- **Widgets:** Total Audit Events · Successful Actions · Unique Contributors · Action Types · Audit Log History.
- **Columns (`onboarding_audit_log`):** `audit_event_id`, `dataflow_group_id`, `action_type` (`CREATE` · `UPDATE` · `VALIDATE_ONLY`), `environment`, `onboarded_by`, `onboarded_at`, `spec_version`, `client_context_json`, `status`, `error_message`, `raw_spec_payload`.
- **Use it to:** answer "who changed this group and when", recover the exact spec that was onboarded (`raw_spec_payload`), and see failed onboarding attempts with their error text. This is the execution history an "agent console" would show: every onboarding, human or agent-driven, lands here.

**Action checklist**

- [ ] A `FAILED` row: the `error_message` is the validator's text; the [Removed & rejected](../reference/json/removed.md) page explains the rejection families.
- [ ] Restore a previous spec: copy `raw_spec_payload` into the Spec Builder via **Open spec → upload**.

## Raw JSON

- **Widgets:** `flow_kind`, `dataflow_group_id`, `dataflow_id` selectors · flows, pick one to inspect · raw JSON payload (click a cell, then Ctrl+C to copy).
- **Use it to:** read the persisted `source_config_json` / `target_config_json` / `source_inputs_json` / `target_configs_json` documents exactly as the engine will read them. `${param}` placeholders are still raw here; `{{catalog}}`/`{{env}}` are already resolved.

## Operating the dashboard

```bash
databricks bundle deploy -t <target> -p <profile> --select dashboards.flowx_control_dashboard
databricks bundle generate dashboard --resource flowx_control_dashboard --force   # after UI edits
```

Queries keep **bare** table names; `dataset_catalog`/`dataset_schema` inject `<catalog>.config` per target. Adding a page that reads an observability view from this dashboard needs the two-part form `observability.v_x`, which overrides only the schema.

## Related

- [Observability dashboard](observability_dashboard.md) · [Genie space](genie.md) · [Console overview](index.md)
- [Master configuration reference](../reference/json/index.md) — every JSON column here, attribute by attribute
- [Code reference · control_plane](../reference/code/control_plane.md)
