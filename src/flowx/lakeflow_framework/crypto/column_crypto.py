"""Column-level AES encryption/decryption at rest, keyed off Unity Catalog secrets.

Keys are referenced via the three-level (``secret_catalog.secret_schema.secret_key``) UC
secret machinery in :mod:`common.crypto.secrets` -- never a classic workspace-level scope.

**Real bug found via live deployment -- never embed a literal ``secret(...)`` SQL call
inside the same expression that materializes a Lakeflow Declarative Pipeline dataset.**
The original implementation built ``aes_encrypt(CAST(col AS STRING), secret('scope',
'key'), 'MODE')`` as one SQL-text expression via ``F.expr(...)`` (keeping the resolved key
value itself off the Python driver, by design). Confirmed live: this silently corrupts the
resulting ciphertext -- 100% of encrypted values decoded back to a byte-length-inflated
string riddled with ``U+FFFD`` replacement-character bytes (the unmistakable signature of
a lossy UTF-8 decode/re-encode round trip), a corruption invisible to `spec_08`'s only
existing check (`test_bronze_ssn_and_email_are_encrypted_not_plaintext`, which only ever
confirmed "not plaintext," never actually decrypted and round-tripped a value) until
`spec_20`'s exhaustive suite added a genuine round-trip assertion. Root cause: Databricks'
platform-wide credential-redaction machinery (``spark.redaction.regex``, which matches the
keyword ``secret`` among others) treats any expression whose *query text* contains a
``secret(...)`` call as credential-bearing and redacts/mutates it as part of Lakeflow's
per-dataset metrics/observability pipeline -- confirmed via a documented, Databricks
support-acknowledged issue with the exact same symptom
(https://community.databricks.com/t5/data-engineering/redacted-possible-secret-access-key-as-part-of-column-value/td-p/55086).
Isolated, non-DLT-wrapped Structured Streaming writes using the identical inline-``secret()``
SQL text never reproduced this -- it is specific to Lakeflow's own logging/observability
layer, not `aes_encrypt`/`secret()` in general.

Fixed per Databricks support's own confirmed workaround: resolve the secret's plaintext
value once via :func:`common.crypto.secrets.resolve_secret_value` *before* building the
encryption/decryption expression, then pass it through PySpark's native
``functions.aes_encrypt``/``aes_decrypt`` column functions as a literal ``Column`` value
(``F.lit(...)``) rather than as interpolated SQL text -- no ``secret(`` substring ever
appears in the expression driving the write, so the redaction trigger never fires.
``resolve_secret_value`` itself no longer uses the SQL ``secret()`` function at all (a
second, related manifestation of this same redaction issue hit its own standalone
resolution query too -- see that function's docstring); it resolves via
``dbutils.secrets.get()`` instead. This does mean the resolved key value now exists as a
Python string on the driver for the duration of one flow's graph-definition call (the same
trade-off :func:`common.crypto.secrets.resolve_secret_value` already makes for ZIP archive
passphrases) -- an intentional trade against the alternative of encryption being silently
non-functional.
"""

from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from flowx.lakeflow_framework.crypto.secrets import assert_safe_identifier, resolve_secret_ref
from flowx.lakeflow_framework.exceptions import CryptoError

_SUPPORTED_AES_MODES = {"GCM", "CBC", "ECB"}


def apply_aes_column_encryption(
    df: DataFrame, encrypted_columns_config: List[Dict[str, Any]]
) -> Tuple[DataFrame, Dict[str, str]]:
    """Encrypt configured columns at rest using ``aes_encrypt`` keyed off a Unity Catalog secret.

    Parameters
    ----------
    df:
        Input DataFrame.
    encrypted_columns_config:
        List of column configs, each shaped like::

            {
              "column_name": "ssn_raw",
              "output_column": "ssn_encrypted",   # optional, defaults to column_name
              "mode": "GCM",                       # GCM (default), CBC, or ECB
              "source_data_type": "string",        # optional, see below
              "secret": {"secret_catalog": "security", "secret_schema": "keys", "secret_key": "pii_encryption_key"}
            }

        ``source_data_type`` (v1.4.0) is the **declared** original type of ``column_name`` --
        the type it had before encryption replaced it with ciphertext binary. It is optional
        and safely defaulted: when absent, the type Spark actually reports for the column at
        this moment is used instead, which is exactly what this function has always done, so
        every pre-v1.4.0 config behaves identically and no spec needs editing.

        Declaring it buys a check that omitting it cannot give you. The captured type becomes
        the Unity Catalog ``original_data_type`` column tag (see the Returns section), and that
        tag is what :func:`apply_aes_column_decryption` validates a downstream
        ``decrypted_columns[].cast_to_type`` against. With no declaration, whatever the source
        happens to deliver today becomes the tag -- so a source column that quietly changes from
        ``string`` to ``int`` silently re-tags, and the *decrypt* side starts failing later, far
        from the cause. With a declaration, the disagreement is caught here, at the point of
        encryption, naming both types. Mismatches are a ``CryptoError``: continuing would tag the
        column with a type the spec does not claim it has, which is the one outcome that helps
        nobody.

    Returns
    -------
    tuple[DataFrame, dict[str, str]]
        ``df`` with each configured column replaced (or added, if ``output_column``
        differs) by its ciphertext (base64-encoded binary) form, plus a
        ``{output_column: original_spark_type}`` map -- encryption replaces a column's
        physical type with ciphertext binary, so the caller (``engine/flow_registration.py``)
        persists this as a Unity Catalog ``original_data_type`` column tag once the target
        table is materialized, so a downstream ``decrypted_columns[].cast_to_type`` can be
        validated against the true original type. The map carries the DECLARED
        ``source_data_type`` when one was given, and the observed Spark type otherwise -- they
        are identical by the time either is returned, because a disagreement raises rather than
        picking a winner.

    Raises
    ------
    CryptoError
        If a config entry is missing required keys, a declared ``source_data_type`` disagrees
        with the column's actual Spark type, or the encryption expression fails to apply
        (unknown column, unsafe identifier, unsupported mode).
    """
    result_df = df
    spark = df.sparkSession
    original_types: Dict[str, str] = {}
    for col_config in encrypted_columns_config:
        try:
            column_name = assert_safe_identifier(col_config["column_name"], "column_name")
            output_column = assert_safe_identifier(col_config.get("output_column", column_name), "output_column")
            mode = col_config.get("mode", "GCM")
            if mode not in _SUPPORTED_AES_MODES:
                raise ValueError(f"Unsupported AES mode '{mode}' for column '{column_name}'")
            if column_name not in result_df.columns:
                raise ValueError(f"Column '{column_name}' configured for encryption not present in DataFrame")

            observed_type = dict(result_df.dtypes)[column_name]
            # .get(), not ["..."]: source_data_type is optional and its absence is the documented
            # default path, not a missing-key error. Compared case-insensitively and
            # whitespace-trimmed because "DECIMAL(18,2)" and "decimal(18,2)" are the same type and
            # a spec author should not have to match Spark's own casing to be believed.
            declared_type = col_config.get("source_data_type")
            if declared_type is not None and str(declared_type).strip().lower() != observed_type.strip().lower():
                raise CryptoError(
                    f"encrypted_columns source_data_type mismatch for column '{column_name}': the spec declares "
                    f"source_data_type={str(declared_type)!r} but Spark reports {observed_type!r} for this column. "
                    "Either the source's type changed, or the declaration is stale. Fix: correct "
                    f"source_data_type to {observed_type!r}, or remove it to accept whatever the source delivers."
                )
            original_types[output_column] = str(declared_type).strip() if declared_type is not None else observed_type
            resolved_key = resolve_secret_ref(spark, col_config["secret"])
            result_df = result_df.withColumn(
                output_column,
                F.aes_encrypt(F.col(column_name).cast("string"), F.lit(resolved_key), F.lit(mode)),
            )
        except KeyError as exc:
            raise CryptoError(f"Missing required encryption config key {exc} in {col_config}") from exc
        except CryptoError:
            # Already a fully-formed, self-explaining CryptoError (the source_data_type mismatch
            # above) -- re-wrapping it would bury that message inside a generic one. Mirrors
            # apply_aes_column_decryption, which re-raises its own cast_to_type mismatch the same
            # way and for the same reason.
            raise
        except Exception as exc:  # noqa: BLE001
            raise CryptoError(f"Failed to apply AES encryption for column config {col_config}: {exc}") from exc

    return result_df, original_types


def apply_aes_column_decryption(
    df: DataFrame, decrypted_columns_config: List[Dict[str, Any]], original_type_tags: Optional[Dict[str, str]] = None
) -> DataFrame:
    """Decrypt configured ciphertext columns using ``aes_decrypt`` keyed off a Unity Catalog secret.

    Parameters
    ----------
    df:
        Input DataFrame containing AES-encrypted binary columns.
    decrypted_columns_config:
        Same shape as ``apply_aes_column_encryption``'s ``encrypted_columns_config``, plus a
        required ``cast_to_type`` naming the Spark type to cast the decrypted plaintext to.
    original_type_tags:
        Optional ``{column_name: original_data_type}`` map (read from the source column's
        Unity Catalog ``original_data_type`` tag, if present -- see
        ``engine/flow_registration.py``). When supplied, a ``cast_to_type`` that disagrees
        with the tagged original type raises ``CryptoError`` naming both, rather than
        silently producing a wrongly-typed column.

    Returns
    -------
    DataFrame
        ``df`` with each configured column replaced by its decrypted plaintext, cast to
        ``cast_to_type``.

    Raises
    ------
    CryptoError
        If a config entry is missing required keys, ``cast_to_type`` conflicts with the
        tagged original type, or the decryption expression fails to apply.
    """
    result_df = df
    spark = df.sparkSession
    original_type_tags = original_type_tags or {}
    for col_config in decrypted_columns_config:
        try:
            column_name = assert_safe_identifier(col_config["column_name"], "column_name")
            output_column = assert_safe_identifier(col_config.get("output_column", column_name), "output_column")
            mode = col_config.get("mode", "GCM")
            cast_to_type = col_config["cast_to_type"]
            if column_name not in result_df.columns:
                raise ValueError(f"Column '{column_name}' configured for decryption not present in DataFrame")

            tagged_type = original_type_tags.get(column_name)
            if tagged_type and tagged_type.lower() != cast_to_type.lower():
                raise CryptoError(
                    f"decrypted_columns cast_to_type mismatch for column '{column_name}': configured "
                    f"cast_to_type={cast_to_type!r} but the encrypted column's tagged original_data_type is "
                    f"{tagged_type!r}. Fix: set cast_to_type to {tagged_type!r} (or re-encrypt the source "
                    "column with the type you actually want decrypted_columns to produce)."
                )

            resolved_key = resolve_secret_ref(spark, col_config["secret"])
            result_df = result_df.withColumn(
                output_column,
                F.aes_decrypt(F.col(column_name), F.lit(resolved_key), F.lit(mode)).cast(cast_to_type),
            )
        except KeyError as exc:
            raise CryptoError(f"Missing required decryption config key {exc} in {col_config}") from exc
        except CryptoError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise CryptoError(f"Failed to apply AES decryption for column config {col_config}: {exc}") from exc

    return result_df
