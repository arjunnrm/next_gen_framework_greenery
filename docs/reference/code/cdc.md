<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# `cdc`

Change-data-capture dispatch: SCD strategies, snapshots, hashing, change metrics.


6 modules.


## `lakeflow_framework/cdc/change_metrics.py`

Insert/update/delete row counts for a CDC-strategy target, via Delta Change Data Feed.


### Functions

| Signature | Purpose |
|---|---|
| `capture_scd_change_counts(spark: Any, qualified_table: str, starting_version: int, ending_version: int) -> Dict[str, int]` | Return ``{"inserted_count", "updated_count", "deleted_count"}`` for one CDC target. |


## `lakeflow_framework/cdc/comparison_columns.py`

Resolve which columns count as a "change" for CDC comparison purposes.


### Functions

| Signature | Purpose |
|---|---|
| `resolve_comparison_columns(all_columns: Sequence[str], primary_keys: Optional[Iterable[str]], columns_to_check: Optional[Iterable[str]], columns_to_exclude: Optional[Iterable[str]]) -> List[str]` | Resolve the final list of columns that count as a "change" for this CDC flow. |


## `lakeflow_framework/cdc/dispatcher.py`

Single entry point routing a ``cdc_load_strategy`` name to its implementing module.


### Functions

| Signature | Purpose |
|---|---|
| `register_cdc_strategy(flow_id: str, cdc_load_strategy: str, source_view: str, target_table: str, target_catalog: str, target_schema: str, target_config: Dict[str, Any], table_properties: Dict[str, str], is_streaming: bool = False) -> None` | Apply the configured CDC / load strategy on top of a staged source view. |
| `resolve_truncate_and_load_source(source_df: DataFrame, existing_target_provider: Callable[[], Optional[DataFrame]], target_config: Dict[str, Any], flow_label: str, target_label: str) -> DataFrame` | Guard a ``TRUNCATE_AND_LOAD`` flow against an empty source blanking its target. |


## `lakeflow_framework/cdc/hashing.py`

The framework's ONE canonical hashing standard -- ``__framework_hash_key`` / ``__framework_hash_value``.


### Functions

| Signature | Purpose |
|---|---|
| `normalized_hash_input(column: str) -> Column` | One column's canonical, normalized hash input. |
| `deterministic_hash_expression(columns: Sequence[str]) -> Column` | The framework's ONE canonical hash expression. |
| `compute_hash_columns(df: DataFrame, primary_keys: Iterable[str], comparison_columns: Iterable[str]) -> DataFrame` | Add ``__framework_hash_key``/``__framework_hash_value`` to ``df``. |
| `xor_fold_hex_digest(digest_column: Column) -> List[Column]` | Build the per-lane ``bit_xor`` aggregates that fold arbitrarily many digests into one. |
| `assemble_xor_folded_digest(lane_values: Sequence[Optional[str]]) -> str` | Concatenate collected per-lane hex values back into one 64-character digest. |


## `lakeflow_framework/cdc/scd.py`

Slowly Changing Dimension strategies: SCD1/SCD2 (native ``apply_changes``) and SCD3 (derived pivot).


### Functions

| Signature | Purpose |
|---|---|
| `register_scd1(flow_id: str, source_view: str, target_table: str, target_catalog: str, target_schema: str, target_config: Dict[str, Any], table_properties: Dict[str, str]) -> None` | Register an SCD Type 1 (overwrite-on-match) target via ``dlt.apply_changes``. |
| `register_scd2(flow_id: str, source_view: str, target_table: str, target_catalog: str, target_schema: str, target_config: Dict[str, Any], table_properties: Dict[str, str]) -> None` | Register an SCD Type 2 (full history) target via ``dlt.apply_changes``. |
| `register_scd2_reporting_view(target_table: str, target_catalog: str, target_schema: str, table_properties: Dict[str, str]) -> None` | Register ``<target_table>_current``, a friendly-column table over a native SCD2 table. |
| `register_scd3(flow_id: str, source_view: str, target_table: str, target_catalog: str, target_schema: str, target_config: Dict[str, Any], table_properties: Dict[str, str]) -> None` | Implement SCD3 (current/previous columns) as a pivot over an internal SCD2 history table. |


## `lakeflow_framework/cdc/snapshot.py`

Full-snapshot CDC via ``dlt.apply_changes_from_snapshot``, keyed on ``primary_keys``.


### Functions

| Signature | Purpose |
|---|---|
| `register_full_snapshot_cdc(flow_id: str, source_view: str, target_table: str, target_catalog: str, target_schema: str, target_config: Dict[str, Any], table_properties: Dict[str, str], cdc_load_strategy: str, is_streaming: bool = False) -> None` | Register ``apply_changes_from_snapshot`` for full-extract CDC on a natural key. |

