# FAQ

Short answers. For role-specific depth see the [multi-role FAQs](10_multi_role_faqs.md); for traps
the validator cannot catch see [known limitations](13_known_limitations_and_gotchas.md).

## Concepts

??? question "What is a `dataflow_group_id`, and can I change it?"
    It is the primary key of the whole onboarding document — every flow in the spec is upserted
    under it. Re-running onboarding with the same id updates in place. **Changing it creates a
    second, independent set of control-table rows, and the original flows keep running.** Treat it
    as immutable once a pipeline is live.

??? question "I edited my spec but nothing changed. Why?"
    Onboarding and running are separate. Onboarding writes control-table rows; the pipeline builds
    its graph from those rows when it starts. Re-run onboarding, then restart the pipeline.

??? question "What is the difference between a flow and a pipeline?"
    A *flow* is one entry in `ingestion_flows[]`, `transformation_flows[]` or
    `reconciliation_flows[]`. A *pipeline* is the Lakeflow object that executes all flows sharing a
    `dataflow_group_id`.

## Ingestion

??? question "The update reports SUCCESS but writes zero rows."
    Almost always the watched directory is not where files land. With `source_zip_handling` enabled,
    `source_config.path` must equal `source_zip_handling.target_volume_path` — nothing validates
    that they agree.

??? question "Every CSV column arrives as a string."
    Add `"cloudFiles.inferColumnTypes": "true"` to `reader_options`, or declare a `schema_config`.

??? question "The pipeline reprocessed everything after a redeploy."
    `schema_location` moved, so the stream lost its checkpoint identity. Restore the original path.

??? question "Can two flows share a `schema_location`?"
    No. Sharing one corrupts both schemas. Give every flow its own, outside the ingested directory.

## CDC

??? question "SCD2 creates a new version every run with no real change."
    `columns_to_check` includes a non-deterministic column — usually an encrypted one. AES-GCM uses
    a random IV, so ciphertext differs every run and every row looks changed. Restrict
    `columns_to_check` to stable business columns.

??? question "`partition_columns` seems to be ignored."
    Expected. Partitioning, liquid clustering and auto TTL apply only to `APPEND` and
    `TRUNCATE_AND_LOAD`; they are silently skipped for CDC-dispatched strategies.

??? question "The pipeline fails complaining about `sequence_by_column`."
    CDC defaults to `__framework_ingestion_timestamp_utc`, which only exists when
    `capture_technical_metadata` is true. Either re-enable it or set `sequence_by_column` explicitly.

??? question "Why is SCD3 missing from ingestion?"
    It is transformation-only. The Spec Builder greys it out on ingestion flows.

## Storage

??? question "Should I partition or use liquid clustering?"
    Liquid clustering, unless the table is comfortably over ~1 TB. Partitioning a smaller table
    produces many small files and usually makes queries slower. Liquid clustering is capped at
    three columns.

??? question "Is `partition_columns: []` an error?"
    No — it is an explicit statement that the table is unpartitioned, and is distinct from omitting
    the field.

??? question "How do I let Iceberg readers read my table?"
    Add `enable_iceberg_read_uniformity: "true"` to `table_properties`. That works for any
    `target_type`. `storage_format: "iceberg"` is only valid for `batch_table`.

## Data quality and governance

??? question "Quarantined rows go nowhere."
    `quarantine` needs `dq_config.quarantine_table`. Set `record_id_column` too, so rows can be
    traced back.

??? question "Tags never appear on my table."
    Tags are applied post-deployment via `ALTER TABLE SET TAGS`, so they show up after the first
    successful update. If they still do not, the run-as principal lacks `APPLY TAG`.

??? question "Does tagging a column protect it?"
    No. The framework applies tags only — it does not create masking policies or row filters.

## The app

??? question "The app serves nothing at `/`."
    `web/dist/` was not built. Databricks Apps does not build at deploy time; run `npm run build`
    in `databricks-app/web` first.

??? question "Some attributes are missing from the form."
    They are almost certainly gated on a selection you have not made — the form hides what does not
    apply. Turn on **Show attributes not applicable** in the left rail to see everything, each with
    the reason it is inert.

??? question "Where do templates come from?"
    `databricks-app/templates/`, scanned on every request. Drop a `.json` file in and it appears on
    the next load. `index.json` is an optional metadata overlay, not a registration list.

??? question "Will opening and re-saving a spec lose anything?"
    No. Import is an exact inverse of save, driven by the same registry, and attributes the builder
    does not recognise are preserved. This is covered by a round-trip test.

## Operations

??? question "A deploy killed a running pipeline with `ENVIRONMENT_PIP_INSTALL_ERROR`."
    The deploy pruned the superseded wheel from `<artifact_path>/.internal/` while the running
    update was installing it. DABs prunes on a UC Volume exactly as in the workspace (v1.6.0 moved
    the artifact path to `/Volumes/<catalog>/config/wheels`, which does **not** change this), and
    unique per-deploy filenames prevent overwrite-in-place, not removal. The rule is operational:
    **never `bundle deploy` while a pipeline or test wave is running** — see
    [Deploying](onboarding/04_deploying.md).

??? question "Why don't I see the `_staged` / `_src__*` / `_recon__*` tables in my catalog anymore?"
    v1.6.0's Intermediate Object Rule: intermediates are `@dlt.view`s or pipeline-scoped
    **temporary** tables — materialized once per update where read-once demands it, but never
    published to Unity Catalog. Only final sinks (targets, quarantine tables, SCD2 `_current`) and
    the conditional reconciliation audit datasets (`__metrics`/`__mismatch`) are published. The
    datasets still exist inside the pipeline — check the pipeline's graph/event log, not the
    catalog. See [Known limitations O7](13_known_limitations_and_gotchas.md#o7) for the upgrade
    implications.

??? question "How do I keep a shared source-plane node queryable from outside the pipeline?"
    Set **both** `source_plane.catalog` and `source_plane.schema` in the spec — the node is then
    published there as a real table, exactly as pre-v1.6.0. Leaving them `null` (the default) keeps
    it a pipeline-scoped temporary table.

??? question "How do I turn off ALL reconciliation logging, and what do I lose?"
    Set `logging_config: {"run_log_capture": false, "mismatch_log_capture": false}` (or the
    runtime overrides). v1.6.0 makes this a real off-switch: no `recon__*__metrics`/`__mismatch`
    dataset is registered, and no `reconciliation_run_log`, `reconciliation_mismatch_log` **or
    `reconciliation_result`** row is written — the flow persists only to its business targets. You
    lose the control-table audit trail entirely, including FAILED rows; the job/pipeline run state
    and structured log events become your only failure signal. Healing still works.

??? question "Why did onboarding reject `run_log_capture: false` on my flow?"
    Two combinations are rejected (at onboarding and again at graph definition): `dq_config.rules`
    with `run_log_capture: false` — the expectations attach to the `__metrics` dataset, which would
    not exist — and `execution_mode: "pipeline_audit_only"` with **both** capture flags false,
    which would register compute with no output at all. Re-enable the flag, or drop the rules /
    switch the mode. See [Reconciliation §6](07_reconciliation_engine.md#6-runtime-log-controls).

??? question "How do I export CSV files inside the password-protected ZIP instead of JSON-Lines?"
    Set `sink_config.staged_file_format: "csv"` on a `pgp_zip` sink (v1.6.0) — staged files become
    RFC-4180 CSV with a header row (one file per non-empty partition per micro-batch), then
    `post_export_archive` zips them as before; combine with `post_export_archive.secret` for an
    AES-256 password-protected archive. The attribute is rejected on `delta`/`kafka` sinks, which
    have no staging step. Working example: `resources/sample_jobs/onboarding/sample_04_export_encrypt_zip.json`.

??? question "A flow failed but the update still reported SUCCESS."
    `pipelines.maxFlowRetryAttempts` defaults to 5 for triggered pipelines, so a transiently failing
    flow is retried and the update still succeeds.
