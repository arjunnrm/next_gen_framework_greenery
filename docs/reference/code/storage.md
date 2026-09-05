<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# `storage`


2 modules.


## `lakeflow_framework/storage/column_ordering.py`

Reorder a target table's output schema so Delta's default data-skipping statistics -- ``delta.dataSkippingNumIndexedCols``, default 32 -- actually cover the columns queries and joins depend on most.


### Functions

| Signature | Purpose |
|---|---|
| `reorder_columns_for_delta_stats(df: DataFrame, target_config: Dict[str, Any], extra_priority_columns: Optional[Sequence[str]] = None) -> DataFrame` | Move key/clustering/hash columns to the front of ``df``'s schema, before it becomes a target table's actual materialized output. |


## `lakeflow_framework/storage/table_properties.py`

Delta/Lakeflow storage optimization: table properties, Liquid Clustering, UniForm, TTL.


### Functions

| Signature | Purpose |
|---|---|
| `qualified_table_name(catalog: str, schema: str, table: str) -> str` | Build a fully-qualified ``catalog.schema.table`` name for a ``@dlt.table``/``@dlt.view`` ``name=`` argument. |
| `build_table_properties(target_config: Dict[str, Any], target_type: Optional[str] = None) -> Dict[str, str]` | Translate a ``target_config`` dict into Delta/Lakeflow table properties. |
| `build_auto_ttl_kwarg(target_config: Dict[str, Any]) -> Optional[Dict[str, Any]]` | Build the ``auto_ttl`` keyword argument for a ``@dlt.table``/``dlt.create_streaming_table`` call. |
| `build_partition_and_cluster_kwargs(target_config: Dict[str, Any], table_label: Optional[str] = None) -> Dict[str, Any]` | Build the ``partition_cols``/``cluster_by`` keyword arguments for a ``@dlt.table`` call. |

