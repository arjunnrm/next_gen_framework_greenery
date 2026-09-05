<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# `archive`

Superseded implementations retained for reference.


3 modules.


## `lakeflow_framework/archive/pgp_zip_sink.py`

A genuine Lakeflow custom sink (PySpark Data Source Sink API) that writes each streaming micro-batch to a ZIP archive, optionally PGP-encrypted -- registered via ``spark.dataSource.register(PgpZipDataSource)`` and referenced from ``engine/sink_registration.py`` as ``dlt.create_sink(format="pgp_zip", ...)``.


### class `PgpZipCommitMessage`

Commit message for one partition's ``write()`` call.


### class `PgpZipDataSource`

Registered via ``spark.dataSource.register(PgpZipDataSource)`` before any ``dlt.create_sink(format="pgp_zip", ...)`` call references it (see ``engine/sink_registration.py::_register_pgp_zip_datasource_once``).


| Method | Purpose |
|---|---|
| `name() -> str` |  |
| `streamWriter(schema: StructType, overwrite: bool) -> DataSourceStreamWriter` |  |


## `lakeflow_framework/archive/zip_ingestion_pipeline.py`

Multi-ZIP batch ingestion: validate -> extract -> load -> join/transform -> re-archive.


### Functions

| Signature | Purpose |
|---|---|
| `validate_zip_batch(zip_paths: List[str]) -> None` | Validate a batch of input ZIP archives before extracting any of them. |
| `ingest_zip_batch(spark: SparkSession, zip_paths: List[str], extract_dir: str, staging_view_configs: Dict[str, str], join_sql: str, output_table: str, output_csv_dir: str, output_zip_path: str, secret_catalog: Optional[str] = None, secret_schema: Optional[str] = None, secret_key: Optional[str] = None, cleanup_extracted_files: bool = True) -> Dict[str, Any]` | Validate, extract, load, join, write, and re-archive a batch of input ZIP files. |


## `lakeflow_framework/archive/zip_utils.py`

Landing-zone ZIP extraction and egress-sink ZIP compression, both optionally AES-256 encrypted.


### class `ZipDeletePolicy`

Normalized internal representation of ``source_zip_handling.delete_source_after_extract``.


| Method | Purpose |
|---|---|
| `delete_current_archive() -> bool` | True only for ``delete_now`` -- the one action that removes THIS run's own archive. |


### Functions

| Signature | Purpose |
|---|---|
| `extract_encrypted_zip(spark: SparkSession, source_zip_path: str, target_volume_path: str, secret_catalog: Optional[str] = None, secret_schema: Optional[str] = None, secret_key: Optional[str] = None, delete_source_after_extract: bool = True) -> List[str]` | Extract a (optionally AES-256 passphrase-protected) ZIP archive into a UC Volume. |
| `resolve_zip_delete_policy(raw: Any) -> ZipDeletePolicy` | Normalize the raw spec value (``None`` / ``bool`` / ``dict``) into one :class:`ZipDeletePolicy`. |
| `sweep_aged_archives(source_zip_dir: str, zip_file_pattern: str, days: int, exclude_paths: Optional[Iterable[str]] = None) -> List[str]` | Delete every already-aged archive in a landing directory. |
| `compress_and_encrypt_sink(spark: Optional[SparkSession], source_dir: str, output_zip_path: str, secret_catalog: Optional[str] = None, secret_schema: Optional[str] = None, secret_key: Optional[str] = None, passphrase: Optional[str] = None, include_glob_suffixes: Tuple[str, ...] = ('.csv', '.json', '.parquet', '.avro', '.txt')) -> str` | Bundle partitioned egress-sink output files into a single (optionally encrypted) ZIP. |
| `extract_gzip_member(source_path: str, target_volume_path: str, delete_source_after_extract: bool = True) -> List[str]` | Decompress one gzip file into ``target_volume_path``. |

