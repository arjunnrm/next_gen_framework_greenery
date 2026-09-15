<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# Removed & rejected attributes

Everything the onboarding validator rejects **on presence** — a removed key, a removed enum value, a key that is legal only in another execution mode, or a name the framework never had. Generated from the dictionaries in `spec_validator.py`, so the messages below are the exact text the onboarding job prints.


!!! danger "Removals are rejected, never ignored"
    An ignored key still onboards, still writes its control-table row and still runs the pipeline — while quietly doing something other than what the document says. Any attribute that once switched a data-shaping behaviour ON would silently switch it OFF. So presence is the trigger: `"flag": false` is still a statement about a feature that no longer exists.


## Removed attributes

Delete the key and apply the migration the message names.


### `source_config.*`

| Attribute | Status | Rejection message (verbatim) |
|---|---|---|
| `source_config.normalize_column_names` | <span class="fx-badge fx-dep">Removed</span> | removed in v1.4.0 -- column_normalization is now the only switch. Replace normalize_column_names: true with column_normalization: {enabled: true} (add a case key if you were relying on something other than the default 'lower'); delete the key outright if it was false. |

### `target_config.*`

| Attribute | Status | Rejection message (verbatim) |
|---|---|---|
| `target_config.generate_surrogate_key` | <span class="fx-badge fx-dep">Removed</span> | removed in v1.4.0 -- the surrogate-key engine is gone. __framework_surrogate_key is no longer generated for any flow. Declare real primary_keys (SCD1/SCD2/SCD3/FULL_SNAPSHOT_CDC all take them), or use TRUNCATE_AND_LOAD if this source has no key. |
| `target_config.surrogate_key_columns` | <span class="fx-badge fx-dep">Removed</span> | removed in v1.4.0 with the surrogate-key engine -- it scoped a column that is no longer generated. To control what identifies a row, set primary_keys; to control what is compared for change, set columns_to_check/columns_to_exclude. |
| `target_config.surrogate_key_exclude_columns` | <span class="fx-badge fx-dep">Removed</span> | removed in v1.4.0 with the surrogate-key engine -- it scoped a column that is no longer generated. To control what identifies a row, set primary_keys; to control what is compared for change, set columns_to_check/columns_to_exclude. |

### `reconciliation_flows[].*`

| Attribute | Status | Rejection message (verbatim) |
|---|---|---|
| `reconciliation_flows[].recon_mode` | <span class="fx-badge fx-dep">Removed</span> | removed in v1.4.0 -- reconciliation is triggered-only. Every run is a bounded job task: batch reads, and trigger(availableNow=True) for a read_mode 'streaming' side, so the run drains its backlog and finishes. There is no continuous reconciliation mode and no recon_mode widget; delete the key. For continuous coverage, either schedule the reconciliation job on the cadence you need (execution_mode 'job', the default), or set execution_mode to 'pipeline'/'pipeline_audit_only' to run this flow's comparison inside its dataflow group's own Lakeflow pipeline update instead. |
| `reconciliation_flows[].generate_surrogate_key` | <span class="fx-badge fx-dep">Removed</span> | removed in v1.4.0 with the surrogate-key engine. A reconciliation flow matches on its declared match_keys; both sides must carry those columns. |

## Removed enum values

The attribute survives; one of its values does not.


### `target_config.cdc_load_strategy`

| Attribute | Status | Rejection message (verbatim) |
|---|---|---|
| `cdc_load_strategy = FULL_SNAPSHOT_CDC_NO_PK` | <span class="fx-badge fx-dep">Removed</span> | removed in v1.4.0 -- it existed only to consume the surrogate-key engine, hashing every payload column of every row on every run to manufacture a diff key. Use FULL_SNAPSHOT_CDC with target_config.primary_keys (the Databricks-native apply_changes_from_snapshot pattern -- https://docs.databricks.com/aws/en/ldp/cdc), or TRUNCATE_AND_LOAD if this source genuinely has no key to diff on. |

### `source_plane.materialize`

| Attribute | Status | Rejection message (verbatim) |
|---|---|---|
| `materialize = never` | <span class="fx-badge fx-dep">Removed</span> | materialize='never' is deprecated and prohibited under the Single-Read architectural mandate. Remove this setting to default to 'always', ensuring base tables are read once and reused via dlt.read(). |

## Keys rejected in a particular execution mode

Legal in one reconciliation `execution_mode`, rejected on presence in another.


### Rejected when `execution_mode` is `pipeline` or `pipeline_audit_only`

| Attribute | Status | Rejection message (verbatim) |
|---|---|---|
| `reconciliation_flows[].{source_config | target_configs[]}.task_run_id_column` | <span class="fx-badge fx-only">Rejected</span> | engine/run_context.py::resolve_pipeline_run_id has no stable per-update key -- pipelines.id is the PIPELINE id, constant across every update, and narrowing by it would match every row that pipeline has ever written, i.e. a silent no-op. Use filter_condition instead, or set execution_mode to 'job' to keep the standalone engine's per-run task_run_id narrowing. |

### Rejected when `execution_mode` is `job` (the default)

| Attribute | Status | Rejection message (verbatim) |
|---|---|---|
| `reconciliation_flows[].publish_schema` | <span class="fx-badge fx-only">Rejected</span> | publish_schema names the schema where this flow's recon__<reconciliation_id>__<target_id>__classified/__metrics/__mismatch datasets are published inside the hosting Lakeflow pipeline -- a 'job' execution_mode flow has no such datasets at all. Set execution_mode to 'pipeline' or 'pipeline_audit_only', or delete this key. |
| `reconciliation_flows[].dq_config` | <span class="fx-badge fx-only">Rejected</span> | dq_config attaches dlt expectations to the one-row __metrics dataset -- a 'job' execution_mode flow never produces that dataset, so there is nothing to attach an expectation to. Set execution_mode to 'pipeline' or 'pipeline_audit_only', or delete this key. |

## Names the framework never had

Since v1.7.2 an unrecognised attribute is a hard error. These are the wrong names seen most often — usually a generated spec reconstructing field names from memory — and the attribute the validator points you to instead.


| You wrote | Use instead |
|---|---|
| `depends_on_dataflow_group_ids` | no replacement in the spec -- inter-group ordering is a Lakeflow Jobs concern. Express it as a task dependency (depends_on) between the groups' jobs in the bundle, not here. Until v1.7.1 this key was silently unread, so any ordering it appeared to declare was never actually enforced |
| `cdc_config` | target_config.cdc_load_strategy (plus primary_keys / sequence_by_column alongside it) -- the separate cdc_config block was a v1 shape and no longer exists |
| `data_quality` | dq_config |
| `quality_config` | dq_config |
| `expectations` | dq_config.rules |
| `file_format` | source_config.format |
| `infer_schema` | No replacement -- delete it. Auto Loader schema inference is always on; point schema_location at a writable path, or pin types explicitly with schema_config_path |
| `schema_inference` | No replacement -- delete it; see schema_location / schema_config_path |
| `partition_by` | target_config.partition_columns |
| `cluster_by` | target_config.liquid_clustering_columns |
| `primary_key` | target_config.primary_keys (a list, even for a single column) |
| `merge_keys` | target_config.primary_keys |
| `sequence_by` | target_config.sequence_by_column |
| `tags` | governance_tags.table_tags |
| `table_comment` | target_config.table_properties |
| `normalize_columns` | source_config.column_normalization ({enabled, case}) |
| `source_path` | source_config.path |
| `target_path` | target_config.sink_config.path (sink targets only) |
| `flow_id` | dataflow_id (ingestion) or flow_step_id (transformation) |
| `sql` | transformation_sql (transformation flows) or transform_sql (reconciliation flows) |
| `query` | transformation_sql |

## Related

- [Onboarding restrictions & validation rules](../../14_onboarding_restrictions_and_validation_rules.md) — every rule, grouped by flow.
- [v1.4.0 attribute delta](../../v1.4.0_attribute_delta.md) — the release most of these removals shipped in.
- [Docs ↔ code synchronisation](../sync.md) — how this page is kept identical to the validator.
