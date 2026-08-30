"""Integration tests for encryption key rotation semantics (crypto/column_crypto.py).

Encrypting under one key and attempting to decrypt under the other is exactly the property
"rotating a key" depends on: ciphertext produced under an old key must not be readable under
a new one, so if you rotate without re-encrypting existing data, that data becomes (correctly)
unreadable -- which is why real rotations re-encrypt in place.

**Why the secret resolution itself is mocked, not live**: ``crypto/secrets.py::resolve_secret_value``
builds ``dbutils`` via ``pyspark.dbutils.DBUtils(spark)`` -- the documented pattern for
accessing ``dbutils`` from library code. That reconstructed shim works correctly inside a
real Databricks notebook/job/DLT-pipeline execution context (already proven live this session
via ``crypto_abac_exhaustive_test_job``'s Section A GCM/CBC/ECB round-trip checks, which run
*inside* a live pipeline). It does **not** work from a bare local ``pytest`` process connected
via Databricks Connect: `pyspark.dbutils.DBUtils`'s local secrets shim doesn't implement the
Unity Catalog three-level ``catalog=``/``schema=`` keyword form at all, and fails immediately
with ``_SecretsUtil.get() got an unexpected keyword argument 'catalog'`` -- confirmed live,
the exact same class of "dbutils resolves differently by execution context" platform quirk
already documented for the plain-job-task-vs-DLT-pipeline case (see
``notebooks/07_verification/07_verify_crypto_abac_exhaustive.py``'s notes on that). Since
*this* module's actual subject is AES rotation semantics -- not secret resolution, which is
already covered live elsewhere -- ``resolve_secret_ref`` is monkeypatched here to a
deterministic in-memory key map, isolating the concern under test. Everything else
(``F.aes_encrypt``/``F.aes_decrypt``, real Spark execution) is genuine, live Spark Connect.
"""

from unittest.mock import patch

import pytest

from NextGen_Metadata_Framework.lakeflow_framework.crypto.column_crypto import (
    apply_aes_column_decryption,
    apply_aes_column_encryption,
)
from NextGen_Metadata_Framework.lakeflow_framework.exceptions import CryptoError

KEY_V1 = {"secret_catalog": "poc", "secret_schema": "security", "secret_key": "key_rotation_test_key_v1"}
KEY_V2 = {"secret_catalog": "poc", "secret_schema": "security", "secret_key": "key_rotation_test_key_v2"}

_FAKE_KEY_VALUES = {
    "key_rotation_test_key_v1": "v1-32-byte-test-key-aaaaaaaaaaaa",
    "key_rotation_test_key_v2": "v2-32-byte-test-key-bbbbbbbbbbbb",
}


@pytest.fixture(autouse=True)
def _mock_secret_resolution():
    """Redirect resolve_secret_ref to a deterministic in-memory key map -- see module docstring."""

    def _fake_resolve(spark, secret_ref):
        return _FAKE_KEY_VALUES[secret_ref["secret_key"]]

    with patch(
        "NextGen_Metadata_Framework.lakeflow_framework.crypto.column_crypto.resolve_secret_ref",
        side_effect=_fake_resolve,
    ):
        yield


def _encrypted_config(secret):
    return [{"column_name": "payload", "secret": secret}]


def _decrypted_config(secret):
    return [{"column_name": "payload", "cast_to_type": "string", "secret": secret}]


def test_encrypt_then_decrypt_with_the_same_key_round_trips(spark):
    df = spark.createDataFrame([("sensitive-value-1",)], ["payload"])
    encrypted, _original_types = apply_aes_column_encryption(df, _encrypted_config(KEY_V1))
    decrypted = apply_aes_column_decryption(encrypted, _decrypted_config(KEY_V1))
    assert decrypted.collect()[0]["payload"] == "sensitive-value-1"


def test_ciphertext_is_not_readable_under_a_different_key(spark):
    """This is the core property key rotation relies on: old ciphertext must not
    decrypt to the right value under the new key. In practice AES-GCM is even stricter
    than "wrong value" -- it's an authenticated mode, so decrypting with the wrong key
    fails its built-in tag check and raises rather than returning garbage. Either outcome
    (raises, or returns something other than the original plaintext) proves the property;
    this test accepts both rather than assuming GCM's specific stricter behavior, since a
    CBC/ECB-mode column (this framework supports both) would return garbage instead."""
    df = spark.createDataFrame([("sensitive-value-2",)], ["payload"])
    encrypted_under_v1, _ = apply_aes_column_encryption(df, _encrypted_config(KEY_V1))
    decrypted_under_v2 = apply_aes_column_decryption(encrypted_under_v1, _decrypted_config(KEY_V2))

    try:
        result_row = decrypted_under_v2.collect()[0]
    except Exception as exc:  # noqa: BLE001 - GCM's authenticated-mode tag check rejecting the wrong key
        assert "tag" in str(exc).lower() or "AES" in str(exc)
        return
    assert result_row["payload"] != "sensitive-value-2"


def test_two_encryptions_of_the_same_value_produce_different_ciphertext(spark):
    """GCM uses a random IV per call -- documented in docs/10_test_pipeline_2_streaming_cdc.md
    as the reason encrypted columns must never be included in SCD2 columns_to_check. This
    test pins that behavior down directly against the encryption function itself."""
    df = spark.createDataFrame([("same-value",), ("same-value",)], ["payload"])
    encrypted, _ = apply_aes_column_encryption(df, _encrypted_config(KEY_V1))
    rows = encrypted.collect()
    assert rows[0]["payload"] != rows[1]["payload"]


def test_new_data_encrypted_after_rotation_round_trips_under_the_new_key(spark):
    """Simulates the correct rotation procedure: new writes use the new key and must
    round-trip cleanly under it, independent of what happens to old ciphertext."""
    df = spark.createDataFrame([("post-rotation-value",)], ["payload"])
    encrypted_under_v2, _ = apply_aes_column_encryption(df, _encrypted_config(KEY_V2))
    decrypted_under_v2 = apply_aes_column_decryption(encrypted_under_v2, _decrypted_config(KEY_V2))
    assert decrypted_under_v2.collect()[0]["payload"] == "post-rotation-value"


def test_cast_to_type_mismatch_against_a_tagged_original_type_raises_actionable_error(spark):
    """decrypted_columns[].cast_to_type is cross-checked against original_data_type when the
    caller supplies that tag map (see engine/flow_registration.py) -- proves the error names
    the column, the configured type, the tagged type, and the fix, not just a bare failure."""
    df = spark.createDataFrame([("sensitive-value-3",)], ["payload"])
    encrypted, _ = apply_aes_column_encryption(df, _encrypted_config(KEY_V1))

    config = [{"column_name": "payload", "cast_to_type": "int", "secret": KEY_V1}]
    with pytest.raises(CryptoError) as exc_info:
        apply_aes_column_decryption(encrypted, config, original_type_tags={"payload": "string"})
    message = str(exc_info.value)
    assert "payload" in message
    assert "int" in message
    assert "string" in message
