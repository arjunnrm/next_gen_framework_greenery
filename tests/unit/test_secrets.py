"""Unit tests for crypto/secrets.py's identifier-safety guard -- pure Python, no Spark needed."""

import pytest

from NextGen_Metadata_Framework.lakeflow_framework.crypto.secrets import (
    assert_safe_identifier,
    qualified_secret_label,
)


@pytest.mark.parametrize("identifier", ["poc", "bronze_finance", "raw_txn", "_leading_underscore", "Mixed123Case"])
def test_valid_identifiers_pass_through_unchanged(identifier):
    assert assert_safe_identifier(identifier) == identifier


@pytest.mark.parametrize(
    "malicious_identifier",
    [
        "poc; DROP TABLE x",
        "poc.config",  # dots not allowed -- must be a single unquoted identifier segment
        "poc`; SELECT 1--",
        "poc OR 1=1",
        "poc'--",
        "",
        "poc space",
    ],
)
def test_unsafe_identifiers_raise_value_error(malicious_identifier):
    with pytest.raises(ValueError, match="Unsafe or malformed"):
        assert_safe_identifier(malicious_identifier)


def test_non_string_identifier_raises_value_error():
    with pytest.raises(ValueError, match="must be a string"):
        assert_safe_identifier(123)  # type: ignore[arg-type]


def test_error_message_includes_custom_label():
    with pytest.raises(ValueError, match="Unsafe or malformed catalog"):
        assert_safe_identifier("bad; name", label="catalog")


def test_qualified_secret_label_builds_readable_label():
    assert qualified_secret_label("security", "keys", "pii_encryption_key") == "security.keys.pii_encryption_key"


def test_qualified_secret_label_rejects_unsafe_component():
    with pytest.raises(ValueError):
        qualified_secret_label("security; DROP", "keys", "pii_encryption_key")


def test_qualified_secret_label_rejects_unsafe_schema():
    with pytest.raises(ValueError):
        qualified_secret_label("security", "keys; DROP", "pii_encryption_key")
