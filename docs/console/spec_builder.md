# :material-application-cog: Spec Builder app · tab by tab

**One Databricks App renders the whole attribute registry as a guided form, validates as you type, previews the exact JSON or YAML that will be written, saves it to a Volume or Workspace path, and triggers the onboarding job.** This page walks every tab, phase and action. The registry it renders is the same one that generates the [master configuration reference](../reference/json/index.md), so a field in the form and an entry in the reference always agree.

!!! abstract "Quick links"
    - Getting-started version of this page: [Using the Spec Builder app](../onboarding/03_spec_builder_app.md)
    - Field-level meaning of every input: [Master configuration reference](../reference/json/index.md) · [Spec tree view](../reference/json/tree.md)
    - What the form cannot catch: [Known limitations & gotchas](../13_known_limitations_and_gotchas.md)
    - Source: `databricks-app/web/src/{Builder.jsx,Shell.jsx,registry.js}`, phases in `databricks-app/config/phases/*.json`, server in `databricks-app/server/`

## Layout

<div class="fx-mock" markdown="0">┌──────────────────────────────────────────────────────────────────────────────────────┐
│ FlowX Onboarding   <span class="fx-tab active">Ingestion</span><span class="fx-tab">Transformation</span><span class="fx-tab">Reconciliation</span><span class="fx-tab">Observability</span>   [Open spec] [Docs ↗] [☾]  │
├───────────────┬──────────────────────────────────────────────┬───────────────────────┤
│ Access        │  Identity › Source › Reader › Load › Protect │  Preview  JSON|YAML   │
│  ✔ volume     │                                              │  {                    │
│  ✔ workspace  │  ┌ Identity ─────────────────── 6 attributes │    "dataflow_group_id"│
│ Sections      │  │ dataflow_id*        [df_orders_ingest   ] │    "ingestion_flows": │
│  • Spec root  │  │ source_system       [sap               ]ⓘ│      [ { ...          │
│  • Flows      │  │ source_type*        [autoloader ▾      ]ⓘ│                       │
│    df_orders  │  └────────────────────────────────────────── │  [raw] [switch]       │
│  + add flow   │  ┌ Source · autoloader ────────────────────  │                       │
│ ☐ Show N/A    │  │ path*  [/Volumes/{{catalog}}/landing/… ]ⓘ│  [Save ▾] [Onboard ▸] │
└───────────────┴──────────────────────────────────────────────┴───────────────────────┘</div>

| Area | What it does |
|---|---|
| **Header tabs** | Switch the working flow kind: Ingestion, Transformation, Reconciliation, Observability. Each tab owns a list of flows in the left rail. |
| **Left rail · Access** | Live check of the storage roots the app may read and write (Volume, Workspace). `re-check` re-probes them. |
| **Left rail · Sections** | Spec root, then one entry per flow of the current kind with a phase progress indicator; **add flow** appends an empty one. |
| **Show attributes not applicable** | Off by default: the form shows only what applies to the current selections. On: every field is visible, inapplicable ones greyed, marked `N/A`, read-only and labelled with the reason. Read-only is deliberate: those values are never written, so accepting input would silently lose it. |
| **Centre** | The current phase's sections, each with an attribute count and a **docs** link into this wiki. |
| **Right rail** | Live preview of exactly what **Save** writes; toggle JSON/YAML with **switch**, open the full payload with **raw**. |
| **ⓘ attribute inspector** | Purpose, why it matters, a sample, best practice, known errors and Databricks documentation links for the field, drawn from `config/attribute_knowledge.json`, the same content as the reference pages. |

## Ingestion tab

Phases from `config/phases/ingestion.json`. Each phase is a step; the flow's progress marker in the left rail advances as required fields are filled.

=== "Identity"

    Sections: **identity**, **dataset**.

    - `dataflow_id` <span class="fx-badge fx-req">Required</span>, `source_system`, `source_database`, `source_table_name`, `source_description`
    - `target_catalog`, `target_schema`, `target_table`, `target_type` <span class="fx-badge fx-req">Required</span>

    **Checklist**

    - [ ] `dataflow_id` follows the `df_<entity>_<purpose>` convention; it is the row key in `ingestion_flow_spec`.
    - [ ] `target_type` is `streaming_table` unless you know why it is not (sinks are `sink`/`external_sink`).

=== "Source"

    Sections: **srctype**, then one of **src_auto** / **src_zb** / **src_asn1** depending on `source_type`, plus **zip**.

    - `source_type` <span class="fx-badge fx-req">Required</span> selects the section shown: `autoloader` (`path`, `format`, `schema_location`, `file_pattern`, `reader_options`, `schema_evolution_mode`, `max_bytes_per_trigger`), `zerobus` (`source_catalog`, `source_schema`, `source_table`, `starting_version`), `asn1` (`asn1_schema_path`, `asn1_codec`, `asn1_pdu_name`).
    - **zip**: `source_zip_handling.*` including `pre_extraction_decryption` for PGP archives.

    **Checklist**

    - [ ] Every Auto Loader flow has its **own** `schema_location`, outside the ingested directory.
    - [ ] With ZIP handling on, `source_config.path` equals `source_zip_handling.target_volume_path`. Nothing validates this; a mismatch is a silent zero-row pipeline.

=== "Reader"

    Sections: **src_common**, **src_norm**, **src_nested**, **retention**.

    - `capture_technical_metadata`, `remove_dups` + `dedup_watermark`, `data_standardization_sql`
    - `column_normalization.{enabled, case}`, `schema_config_path`
    - `explode_columns` (via the `explode_mode` helper), `auto_flatten_all`, `json_string_columns`
    - `landing_retention_policy.{clean_source, archive_path, retention_days}`

    **Checklist**

    - [ ] `data_standardization_sql` entries end with `AS <column>`; they are expressions, not statements.
    - [ ] DQ rules written later refer to **post-normalisation** column names.

=== "Load strategy"

    Sections: **cdc** (a tab strip, one tab per strategy), **storage**.

    - Pick `cdc_load_strategy`; only that strategy's parameters render (`primary_keys`, `sequence_by_column`, `columns_to_check`, `columns_to_exclude`, `cdc_operation_column`, `cdc_operation_mapping`, `empty_target_if_source_empty`). SCD3 is greyed on ingestion flows.
    - **storage**: `storage_format`, `partition_columns` (via `partition_mode`), `liquid_clustering_columns`, `table_properties`, `auto_ttl`, `generate_hash_columns`.

    **Checklist**

    - [ ] No strategy selected means the spec carries **no** `cdc_load_strategy` and onboarding rejects it. Pick one.
    - [ ] Partitioning and liquid clustering apply to `APPEND` and `TRUNCATE_AND_LOAD` only; the form marks them N/A otherwise.

=== "Protect"

    Sections: **enc**, **sink**, **dq**, **gov**.

    - `encrypted_columns[]` with Unity Catalog three-level `secret` references
    - `sink_config` (rendered only for `sink`/`external_sink` targets)
    - `dq_config.rules[]` (`rule_id`, `expression`, `action`), `quarantine_table`, `record_id_column`
    - `governance_tags.table_tags`, `governance_tags.column_tags[]`

    **Checklist**

    - [ ] Never type a secret value; the registry's `forbidden_keys` (`password`, `token`, `private_key`, ...) refuse them and the validator rejects literals.
    - [ ] A `quarantine` action creates a `<target_table>_quarantine` sibling; give `record_id_column` a real business key.

## Transformation tab

Phases from `config/phases/transformation.json`: **Identity** (identity, dataset), **Inputs** (inputs, decrypt), **Transform** (sql), **Load strategy** (cdc, storage), **Protect** (enc, sink, dq, gov).

=== "Identity"

    - `flow_step_id` <span class="fx-badge fx-req">Required</span>, `dataflow_id` <span class="fx-badge fx-req">Required</span>, `source_description`
    - `target_catalog`, `target_schema`, `target_table`, `target_type` <span class="fx-badge fx-req">Required</span>

=== "Inputs"

    - `source_inputs[]`: `input_name`, `table` (one box, three-part name), `is_streaming`, `watermark.{event_time_column, delay_threshold}`
    - `decrypted_columns[]` per input: `column_name`, `cast_to_type`, `mode`, `secret`

    **Checklist**

    - [ ] `input_name` is the alias `transformation_sql` refers to; the framework binds it through `dlt.read`/`dlt.read_stream`, never a raw path.
    - [ ] A `MERGE`-written upstream (SCD1/SCD2) cannot be read as a stream; set `is_streaming: false`.

=== "Transform"

    - `transformation_sql` <span class="fx-badge fx-req">Required</span>, edited in a SQL widget with `${param}` placeholders allowed.

    **Checklist**

    - [ ] Never wrap `${param}` in your own quotes; string parameters are quoted for you.

=== "Load strategy"

    Same strip as ingestion, with **SCD3** enabled here (transformation only). `TRUNCATE_AND_LOAD` requires a `materialized_view`/`batch_table` target.

=== "Protect"

    Same sections as ingestion: encryption on write, sink egress, DQ, governance tags.

## Reconciliation tab

Phases from `config/phases/reconciliation.json`: **Identity**, **Datasets** (rsource, rtargets), **Matching**, **Quality** (rdq).

=== "Identity"

    - `reconciliation_id` <span class="fx-badge fx-req">Required</span>, per-flow `dataflow_group_id`, `execution_mode` (`job` · `pipeline` · `pipeline_audit_only`), `publish_schema`, `two_tier_verification`, `logging_config.{run_log_capture, mismatch_log_capture}`, `error_handling.on_failure`

=== "Datasets"

    - **rsource**: `source_config.{type, table, read_mode, task_run_id_column, filter_condition, data_standardization_sql, hash_precomputed}`
    - **rtargets**: `target_configs[]` rendered as three boxes (`target_catalog`, `target_schema`, `target_table`) that the app recomposes into `{"type": "table", "table": "cat.sch.tbl"}` on save, plus `target_id`, `comparison_direction`, `append_target_table`, `read_mode`, `hash_precomputed`, `filter_condition`

    **Checklist**

    - [ ] In `pipeline`/`pipeline_audit_only` mode the form greys `read_mode: streaming` and `task_run_id_column`; both are rejected on presence.
    - [ ] `publish_schema` and `dq_config` are rejected in `job` mode; the form marks them N/A there.

=== "Matching"

    - `match_keys` <span class="fx-badge fx-req">Required</span>, `compare_columns`, `transform_sql`

=== "Quality"

    - `dq_config.rules[]` on the one-row `__metrics` dataset (`action: quarantine` is refused for reconciliation).

## Spec root and Observability

The **Observability** header tab and the **Spec root** rail entry share `config/phases/spec.json`: **Spec root** (root, tmplvars) and **Observability** (obsmaster, obs, fwcols).

=== "Spec root"

    - `dataflow_group_id` <span class="fx-badge fx-req">Required</span>, `pipeline_parameters` (key/value), `spark_config` (keys must start with `spark.`)
    - **tmplvars**: the `{{catalog}}` and `{{env}}` template variables, defaulted from `config/index.json`, resolved once at onboarding.

=== "Observability"

    - **obsmaster**: the `observability_enabled` helper; off means no `observability[]` array is written.
    - **obs**: one destination per repeat: `id`, `type`, `mode`, `enabled`, `destination_config.*`, `auth.*`, `retry.*`, `timeout_ms`. `event_log_tables` is edited as a list of three-part names and is only legal on `mode: continuous`.
    - **fwcols**: a read-only note listing the framework-generated columns every target table gains.

## Open, save, templates

| Action | What happens |
|---|---|
| **Open spec** | Upload a `.json`/`.yaml`, or type a path under a configured root and **Browse** to navigate, or **Validate** to parse and check a file without loading it. Import is lossless: attributes the builder does not recognise are preserved, not dropped. |
| **Templates** | The browser lists built-in presets and everything under `databricks-app/templates/`; drop a `.json` there and it appears on next load, described from its own content. **Create from scratch** starts empty. |
| **Save** | Download, or write to a Unity Catalog Volume or a Workspace path, as JSON, YAML or both. File name follows `filename_template` (`{dataflow_group_id}_{timestamp}.json`). Only roots listed in `config/index.json` are reachable; path traversal is rejected server-side. |

## Run onboarding from the app

**Onboard** saves first, then runs the onboarding job through the app's on-behalf-of client with `spec_file_path`, `catalog`, `env` and `action_type`. The stage list mirrors the job:

<div class="fx-mock" markdown="0">▸ Upload & stage spec (dfg_orders_20260914.json)
▸ Validate spec against UC schema & constraints
▸ Upsert control table metadata for dfg_orders
▸ Register datasets & Delta tables in catalog 'flowx'
▸ Apply governance tags & lineage
✔ Onboarding completed successfully          [Open job run in Databricks ↗]</div>

**Checklist before you press it**

- [ ] The preview shows the `dataflow_group_id` you intend to upsert; a changed id creates a second, independent group.
- [ ] `action_type`: `VALIDATE_ONLY` first on a live catalog, then `CREATE` (new group) or `UPDATE` (existing).
- [ ] The confirmation text names the environment label; the app never onboards to a target you did not select.

After it completes, onboarding has written rows only. Run the pipeline (`databricks bundle run <pipeline> -t <target>`) for the change to take effect.

## Server API the UI calls

All routes sit under the app's `/api` prefix and are what an automation could call instead of the form.

| Route | Purpose |
|---|---|
| `GET /config`, `GET /attribute-knowledge`, `GET /access` | App config, the inspector knowledge base, storage-root access probe |
| `GET /templates`, `GET /templates/{id}` | Template catalogue |
| `POST /validate`, `POST /import`, `POST /render`, `POST /diff`, `POST /predicate` | Validate a document, import (lossless), render JSON/YAML, diff, evaluate applicability predicates |
| `GET /list`, `GET /read`, `GET /resolve`, `POST /write` | Browse, read and write specs under the configured roots |
| `GET /{action}/parameters`, `POST /{action}/run`, `GET /runs/{id}`, `POST /runs/{id}/cancel` | The `validate` and `onboard` actions from `config/index.json`, job polling and cancel |
| `GET /docs/...` | This wiki, served from `databricks-app/docs_site/` |

## Configuration

`databricks-app/config/index.json` holds storage roots, template variables, actions and docs settings.

```json
{
  "spec_storage": {
    "roots": [
      { "id": "vol_specs", "kind": "volume",
        "path": "/Volumes/{{catalog}}/flowx/onboarding_specs/", "read": true, "write": true },
      { "id": "ws_specs", "kind": "workspace",
        "path": "/Workspace/Shared/flowx/specs/", "read": true, "write": true }
    ],
    "default_root": "vol_specs",
    "allowed_extensions": [".json", ".yaml", ".yml"],
    "filename_template": "{dataflow_group_id}_{timestamp}.json"
  },
  "actions": {
    "validate": { "mode": "local", "stages": ["Upload spec", "Resolve template variables", "Schema validation", "Cross-field validation", "Report"] },
    "onboard":  { "mode": "job", "confirm": true, "requires": ["validate_passed"] }
  },
  "docs": { "mode": "embedded", "base_url": "/docs/" }
}
```

!!! note "Volume path in the app config versus the bundle"
    The bundle declares the spec Volume as `${var.catalog}.${var.spec_schema}.${var.spec_volume}`, defaulting to `<catalog>.config.onboarding_specs`. Keep the app's `vol_specs.path` pointing at the Volume the bundle actually creates for your target.

## Run locally, deploy

```bash
cd databricks-app
pip install -r requirements.txt
cd web && npm install && npm run build && cd ..
FLOWX_FAKE_DBX=1 uvicorn server.app:app --port 8000     # every workspace call stubbed

# deploy: bundle deploy uploads; bundle run is what actually restarts the app on new code
databricks bundle deploy -t <target> -p <profile>
databricks bundle run flowx_onboarding_app -t <target> -p <profile>
```

## Related

- [Console overview](index.md) · [Control metadata dashboard](control_dashboard.md) (see what the app onboarded)
- [Master configuration reference](../reference/json/index.md) · [Removed & rejected attributes](../reference/json/removed.md)
- [Pillars](../pillars/index.md)
