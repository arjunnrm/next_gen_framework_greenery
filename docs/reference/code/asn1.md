<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# `asn1`

BER/DER decoding of binary CDR payloads.


1 modules.


## `lakeflow_framework/asn1/decoder.py`

ASN.1 BER/DER binary decoding for telecom CDR-style sources.


### Functions

| Signature | Purpose |
|---|---|
| `derive_asn1_field_defs(schema_path: str, pdu_name: str) -> List[Dict[str, Any]]` | Derive the Spark output field list for ``pdu_name`` directly from a real ASN.1 module file, via ``asn1tools.parse_files`` introspection -- no hand-authored field list. |
| `make_partition_decoder(module_files: List[str], codec: str, pdu_name: str, field_defs: List[Dict[str, str]], binary_column: str, passthrough_columns: List[str]) -> Callable[[Iterator[pd.DataFrame]], Iterator[pd.DataFrame]]` | Build the ``mapInPandas`` partition function. |
| `decode_asn1_binary_stream(df: DataFrame, schema_path: str, codec: str, pdu_name: str, binary_column: str = 'content') -> DataFrame` | Decode a column of raw ASN.1 BER/DER-encoded binary payloads into structured columns. |

