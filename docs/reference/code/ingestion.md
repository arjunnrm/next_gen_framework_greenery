<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# `ingestion`

Source readers and the Bronze-layer transforms applied on read.


7 modules.


## `lakeflow_framework/ingestion/column_normalization.py`

Opt-in Bronze/raw column-name normalization: trim, case-fold, replace whitespace and special characters with underscores.


### Functions

| Signature | Purpose |
|---|---|
| `resolve_column_normalization(source_config: Dict[str, Any]) -> Tuple[bool, str]` | Resolve ``(enabled, case)`` for one ingestion flow from its raw ``source_config``. |
| `normalize_column_name(name: str, case: str = DEFAULT_NORMALIZATION_CASE) -> str` | Trim, case-fold per ``case``, and replace whitespace/special characters with a single underscore. |
| `normalize_column_names(df: DataFrame, source_config: Dict[str, Any]) -> DataFrame` | Rename every column on ``df`` per :func:`normalize_column_name`, when column normalization is enabled for this flow (default off -- a no-op otherwise, returning ``df`` unchanged). |


## `lakeflow_framework/ingestion/dedup.py`

Opt-in stream-level **full-row** deduplication for ingestion flows (``source_config.remove_dups``).


### Functions

| Signature | Purpose |
|---|---|
| `resolve_dedup_columns(columns: Sequence[str]) -> List[str]` | The full-row dedup subset for ``columns``: everything except the technical columns. |
| `apply_stream_dedup(df: DataFrame, source_config: Dict[str, Any]) -> DataFrame` | Full-row deduplication of ``df``, gated by ``source_config.remove_dups``. |


## `lakeflow_framework/ingestion/json_flattening.py`

Runtime application of ``explode_columns`` -- JSON struct/array flattening for ingestion flows.


### Functions

| Signature | Purpose |
|---|---|
| `apply_explode_columns(df: DataFrame, explode_columns: Optional[List[str]], auto_flatten_all: bool = False) -> DataFrame` | Apply ``explode_columns`` semantics to ``df``. |
| `resolve_auto_flatten_all(source_config: Dict[str, Any]) -> bool` | Resolve ``auto_flatten_all`` for one ingestion flow from its raw ``source_config`` dict. |
| `parse_json_string_columns(df: DataFrame, json_string_columns: Optional[List[Any]]) -> DataFrame` | Parse each named STRING column holding a JSON document into a struct, via ``from_json``. |


## `lakeflow_framework/ingestion/readers.py`

Ingestion source readers: autoloader (cloudFiles), zerobus (Delta stream), asn1 (binary + decode).


### Functions

| Signature | Purpose |
|---|---|
| `resolve_landing_retention_policy(source_config: Dict[str, Any]) -> Dict[str, Any]` | Resolve one flow's ``landing_retention_policy`` block into a fully-defaulted dict. |
| `read_autoloader_source(spark: SparkSession, source_config: Dict[str, Any]) -> DataFrame` | Build a streaming Auto Loader (``cloudFiles``) reader from ``source_config``. |
| `read_zerobus_source(spark: SparkSession, source_config: Dict[str, Any]) -> DataFrame` | Build a streaming reader over an existing Delta table landed via Zerobus. |
| `read_asn1_source(spark: SparkSession, source_config: Dict[str, Any]) -> DataFrame` | Build a streaming binary reader for ASN.1-encoded CDR files, then decode via ``common.asn1``. |
| `base_read_options(source_type: str, source_config: Dict[str, Any]) -> Dict[str, Any]` | Select the base-read subset of ``source_config`` that a source-plane ``ReadIdentity``'s ``options_fingerprint`` is computed over (``sha256`` of canonical JSON over exactly this dict, per the read-once contract) -- see :data:`_BASE_READ_KEYS`. |
| `read_locator(source_config: Dict[str, Any], source_type: str) -> Tuple[str, str]` | Compute the ``(locator_kind, locator)`` half of a source-plane ``ReadIdentity`` for one ingestion ``source_config`` -- see the read-once contract's CANONICAL IDENTITY section (``docs/13``) and ``ReadIdentity`` in ``engine/source_plane.py``. |
| `read_ingestion_source(spark: SparkSession, source_type: str, source_config: Dict[str, Any]) -> DataFrame` | Dispatch to the reader registered for ``source_type``. |


## `lakeflow_framework/ingestion/schema_config.py`

Explicit, external schema definition for Bronze/raw ingestion: type mapping, nullability documentation, Unity Catalog column comments, and source-to-target field renaming.


### Functions

| Signature | Purpose |
|---|---|
| `resolve_schema_config_path(path: str) -> str` | Resolve ``schema_config_path`` to one concrete file. |
| `load_schema_config(dbutils: Any, schema_config_path: str, catalog: str = '', environment: str = '') -> Dict[str, Any]` | Resolve, read, template, and parse a ``schema_config`` file. |
| `apply_schema_config(df: DataFrame, schema_config: Dict[str, Any]) -> DataFrame` | Apply ``schema_config``'s type casts, renames, and column comments to ``df``. |


## `lakeflow_framework/ingestion/standardization_sql.py`

Runtime application of ``data_standardization_sql`` -- a column-expression-only allowlist.


### Functions

| Signature | Purpose |
|---|---|
| `apply_data_standardization_sql(df: DataFrame, expressions: Optional[List[str]]) -> DataFrame` | Apply each configured column expression to ``df``, in order. |


## `lakeflow_framework/ingestion/technical_metadata.py`

Technical audit metadata capture: Auto Loader ``_rescued_data`` + hidden storage metadata, plus the framework-wide ``__framework_ingestion_timestamp_utc`` column.


### Functions

| Signature | Purpose |
|---|---|
| `attach_technical_metadata(df: DataFrame, source_config: Dict[str, Any]) -> DataFrame` | Attach Auto Loader technical audit columns: rescued data + hidden storage metadata. |
| `attach_framework_ingestion_timestamp(df: DataFrame, capture_technical_metadata: bool = True) -> DataFrame` | Attach ``__framework_ingestion_timestamp_utc`` -- a framework-wide, always-consistent processing timestamp, added to **every** ingestion and transformation target (gated by the same ``capture_technical_metadata`` toggle both flow types already expose). |

