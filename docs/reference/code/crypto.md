<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# `crypto`

Column-level AES, PGP, and Unity Catalog secret resolution.


3 modules.


## `lakeflow_framework/crypto/column_crypto.py`

Column-level AES encryption/decryption at rest, keyed off Unity Catalog secrets.


### Functions

| Signature | Purpose |
|---|---|
| `apply_aes_column_encryption(df: DataFrame, encrypted_columns_config: List[Dict[str, Any]]) -> Tuple[DataFrame, Dict[str, str]]` | Encrypt configured columns at rest using ``aes_encrypt`` keyed off a Unity Catalog secret. |
| `apply_aes_column_decryption(df: DataFrame, decrypted_columns_config: List[Dict[str, Any]], original_type_tags: Optional[Dict[str, str]] = None) -> DataFrame` | Decrypt configured ciphertext columns using ``aes_decrypt`` keyed off a Unity Catalog secret. |


## `lakeflow_framework/crypto/pgp.py`

PGP encrypt/decrypt/sign, via ``PGPy`` -- pure Python, no external ``gpg`` binary.


### Functions

| Signature | Purpose |
|---|---|
| `pgp_decrypt(data: bytes, private_key_armored: str, passphrase: Optional[str] = None) -> bytes` | Decrypt a PGP-encrypted binary payload with an ASCII-armored private key. |
| `pgp_encrypt(data: bytes, recipient_public_key_armored: str, sign_with_private_key_armored: Optional[str] = None, sign_passphrase: Optional[str] = None) -> bytes` | Encrypt a binary payload for a recipient's PGP public key, optionally signing it. |
| `pgp_verify(data: bytes, signed_message: bytes, signer_public_key_armored: str) -> bool` | Verify a PGP signature over ``data`` using the signer's ASCII-armored public key. |
| `pgp_decrypt_symmetric(data: bytes, passphrase: str) -> bytes` | Decrypt a passphrase-encrypted (symmetric) PGP payload. |
| `pgp_encrypt_symmetric(data: bytes, passphrase: str, cipher: str = 'AES256') -> bytes` | Encrypt a payload under a shared passphrase (symmetric PGP), ASCII-armored. |


## `lakeflow_framework/crypto/secrets.py`

Unity Catalog secret resolution helpers (v2 schema -- replaces classic workspace scopes).


### Functions

| Signature | Purpose |
|---|---|
| `assert_safe_identifier(name: str, label: str = 'identifier') -> str` | Validate that ``name`` is a safe, unquoted SQL identifier / secret catalog-schema-key name. |
| `qualified_secret_label(secret_catalog: str, secret_schema: str, secret_key: str) -> str` | Build a validated ``catalog.schema.key`` label, for logging/error messages only. |
| `resolve_secret_value(spark: SparkSession, secret_catalog: str, secret_schema: str, secret_key: str) -> str` | Resolve the plaintext value of a Unity Catalog secret. |
| `resolve_secret_ref(spark: SparkSession, secret_ref: dict) -> str` | Resolve a ``{secret_catalog, secret_schema, secret_key}`` dict, as validated by ``onboarding/spec_validator.py::check_secret_ref``. |

