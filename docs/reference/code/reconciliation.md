<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# `reconciliation`

Baseline-versus-target comparison and self-healing.


7 modules.


## `lakeflow_framework/reconciliation/appender.py`

Per-target reconciliation orchestration: match, append, log -- one target at a time.


### Functions

| Signature | Purpose |
|---|---|
| `compute_batch_fingerprint(df: DataFrame, match_keys: List[str]) -> str` | Deterministic sha256-style hash of ``df``'s distinct ``match_keys`` value combinations. |
| `is_target_batch_already_processed(spark: SparkSession, control_schema: str, reconciliation_id: str, target_id: str, fingerprint: str) -> bool` | Check whether this exact target's miss set was already successfully appended. |
| `apply_transform_sql(missing_df: DataFrame, transform_sql: Optional[str], parameters: Optional[Dict[str, Any]] = None, reconciliation_id: str = '', target_id: str = '') -> DataFrame` | Reshape ``missing_df`` (a target's ``source_to_target`` miss set) via the flow-level ``transform_sql``, when configured, to match ``append_target_table``'s own schema. |
| `append_missing_records(missing_df: DataFrame, append_target_table: str) -> int` | Append ``missing_df`` (already ``transform_sql``-reshaped, if configured) into ``append_target_table``, returning the row count appended. |
| `write_run_log_entry(spark: SparkSession, control_schema: str, reconciliation_id: str, target_id: str, run_id: str, fingerprint: str, status: str, metrics: Optional[ReconciliationMetrics] = None, error_message: Optional[str] = None, task_run_id: Optional[str] = None) -> None` | Append one row to ``reconciliation_run_log`` for this target's run. |
| `write_reconciliation_result(spark: SparkSession, control_schema: str, reconciliation_id: str, target_id: str, run_id: str, status: str, metrics: Optional[ReconciliationMetrics] = None, task_run_id: Optional[str] = None) -> None` | Append one row to ``reconciliation_result``. |
| `resolve_log_capture_flags(logging_config: Optional[Dict[str, Any]], recon_run_log_capture: Optional[bool] = None, recon_mismatch_log: Optional[bool] = None) -> Tuple[bool, bool]` | Resolve ``(run_log_capture, mismatch_log_capture)`` for one reconciliation run. |
| `run_target_reconciliation(spark: SparkSession, control_schema: str, reconciliation_id: str, source_df: DataFrame, target_df: DataFrame, target_config: Dict[str, Any], match_keys: List[str], compare_columns: Optional[List[str]], source_hash_precomputed: bool, transform_sql: Optional[str], parameters: Optional[Dict[str, Any]] = None, logging_config: Optional[Dict[str, Any]] = None, task_run_id: Optional[str] = None, recon_run_log_capture: Optional[bool] = None, recon_mismatch_log: Optional[bool] = None, two_tier_verification: bool = True) -> ReconciliationMetrics` | Match, append, and log for exactly one ``target_configs[]`` entry -- the happy path only. |


## `lakeflow_framework/reconciliation/dataset_reader.py`

Dataset reading for reconciliation (``source_config``/``target_configs[]``).


### Functions

| Signature | Purpose |
|---|---|
| `resolve_table_provider(spark: SparkSession, table: str) -> Optional[str]` | Best-effort provider (``delta``, ``parquet``, ``csv``, ...) of a catalog table. |
| `read_reconciliation_dataset(spark: SparkSession, config: Dict[str, Any], parameters: Optional[Dict[str, Any]] = None, task_run_id: Optional[str] = None, in_graph: bool = False) -> DataFrame` | Read one side of a reconciliation comparison (``source_config`` or one ``target_configs[]`` entry). |
| `apply_reconciliation_overlays(df: DataFrame, dataset_config: Dict[str, Any], parameters: Optional[Dict[str, Any]] = None, task_run_id: Optional[str] = None) -> DataFrame` | Apply the post-read overlay chain shared by every reconciliation dataset read. |


## `lakeflow_framework/reconciliation/graph_registration.py`

L3/L4/L5 reconciliation graph registration -- the ``execution_mode: "pipeline"``/ ``"pipeline_audit_only"`` counterpart to ``05_reconciliation_engine.py``'s standalone job-task path.


### Functions

| Signature | Purpose |
|---|---|
| `register_reconciliation_flow(spark: SparkSession, flow_row: Any, plan: SourcePlanePlan, publish_catalog: str, publish_schema: str, control_schema: str, pipeline_update_id: Optional[str] = None, log_capture_overrides: Optional[Dict[str, Optional[bool]]] = None, pipeline_parameters: Optional[Dict[str, Any]] = None) -> None` | Register one ``reconciliation_flow_spec`` row's L3 (prepare) + L4 (compare) + L5 (heal) datasets/flows into the currently-building Lakeflow Declarative Pipeline graph. |


## `lakeflow_framework/reconciliation/matcher.py`

Hash-first matching between a reconciliation flow's source and one of its targets.


### class `ReconciliationFingerprint`

One side's cheap Phase 1 fingerprint (see this module's docstring).


### class `ReconciliationMatchResult`

Per-target output of :func:`match_reconciliation_target`.


### class `ReconciliationClassification`

Lazy, pipeline-safe output of :func:`classify_reconciliation_target` -- the same hash-key join / priority collapse :class:`ReconciliationMatchResult` has always produced, minus the one eager ``.collect()`` that makes it illegal to call from inside a Lakeflow Declarative Pipeline ``@dlt.table`` closure reachable from a streaming read (Lakeflow rule 2).


### Functions

| Signature | Purpose |
|---|---|
| `target_prefixed_column(column: str) -> str` | Name of ``column`` as it appears target-side in :func:`match_reconciliation_target`'s output (``classified_df``/``mismatch_detail_df``) -- shared with ``mismatch_logging.py`` so both modules agree … |
| `prepare_dataset_for_matching(df: DataFrame, match_keys: List[str], compare_columns: Optional[List[str]], hash_precomputed: bool) -> DataFrame` | Ensure ``df`` carries ``__framework_hash_key``/``__framework_hash_value``, computing them inline when ``hash_precomputed`` is ``False``. |
| `compute_side_fingerprint(df: DataFrame) -> ReconciliationFingerprint` | Phase 1: one single-pass aggregate over ``df`` -- ``count(1)`` plus both hash columns' folds. |
| `fingerprints_match(source: ReconciliationFingerprint, target: ReconciliationFingerprint) -> bool` | ``True`` only when ``row_count`` **and** both XOR digests are equal on both sides. |
| `classify_reconciliation_target(source_df: DataFrame, target_df: DataFrame, match_keys: List[str], compare_columns: Optional[List[str]] = None, source_hash_precomputed: bool = False, target_hash_precomputed: bool = False) -> ReconciliationClassification` | Fully lazy hash-key classification of ``source_df`` against ``target_df``. |
| `match_reconciliation_target(source_df: DataFrame, target_df: DataFrame, match_keys: List[str], compare_columns: Optional[List[str]] = None, source_hash_precomputed: bool = False) -> ReconciliationMatchResult` | Compare one prepared source DataFrame against one prepared target DataFrame. |


## `lakeflow_framework/reconciliation/metrics.py`

Reconciliation run metrics -- one ``reconciliation_run_log`` row's worth of counts, per target.


### class `ReconciliationMetrics`

The counts one ``target_configs[]`` entry's reconciliation run reports.


| Method | Purpose |
|---|---|
| `as_dict() -> dict` |  |


## `lakeflow_framework/reconciliation/mismatch_logging.py`

Per-record mismatch detail: ``matcher.py``'s classification, written to ``reconciliation_mismatch_log``.


### Functions

| Signature | Purpose |
|---|---|
| `build_mismatch_rows(mismatch_detail_df: DataFrame, reconciliation_id: str, target_id: str, match_keys: List[str], compare_columns: Optional[List[str]] = None) -> DataFrame` | Pure projection of ``mismatch_detail_df`` (``matcher.py::ReconciliationMatchResult. |
| `write_mismatch_log_rows(spark: SparkSession, control_schema: str, run_id: str, reconciliation_id: str, target_id: str, mismatch_detail_df: DataFrame, match_keys: List[str], compare_columns: Optional[List[str]] = None, task_run_id: Optional[str] = None) -> int` | Project ``mismatch_detail_df`` (``matcher.py::ReconciliationMatchResult.mismatch_detail_df``) into ``reconciliation_mismatch_log`` rows and append them, returning the row count written. |


## `lakeflow_framework/reconciliation/streaming.py`

``read_mode: "streaming"`` reconciliation -- a ``foreachBatch``-driven incremental alternative to ``appender.py``'s batch point-in-time-snapshot + fingerprint-dedup path.


### Functions

| Signature | Purpose |
|---|---|
| `run_streaming_target_reconciliation(spark: SparkSession, control_schema: str, reconciliation_id: str, source_config: Dict[str, Any], target_config: Dict[str, Any], match_keys: List[str], compare_columns: Optional[List[str]], transform_sql: Optional[str], checkpoint_location: str, parameters: Optional[Dict[str, Any]] = None, logging_config: Optional[Dict[str, Any]] = None, task_run_id: Optional[str] = None, recon_run_log_capture: Optional[bool] = None, recon_mismatch_log: Optional[bool] = None, two_tier_verification: bool = True) -> None` | Run one target's reconciliation as a ``foreachBatch`` streaming query. |

