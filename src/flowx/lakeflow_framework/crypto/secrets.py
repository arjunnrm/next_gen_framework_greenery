"""Unity Catalog secret resolution helpers (v2 schema -- replaces classic workspace scopes).

Kept dependency-free (no ``dlt`` import) so it can be unit tested or reused from the
onboarding engine, the pipeline engine, or a plain notebook without pulling in
Lakeflow-specific machinery.

**Design note -- Unity Catalog secrets, three-level namespace:** every secret reference in
this framework (encryption/decryption keys, PGP private/public keys, ZIP archive
passwords, sink credentials) is a Unity Catalog secret, addressed as
``catalog.schema.secret_name`` per
`the Unity Catalog secrets model <https://docs.databricks.com/aws/en/security/secrets/unity-catalog-secrets>`_,
resolved at runtime via ``dbutils.secrets.get(catalog=, schema=, key=)`` -- **not** the SQL
``secret(scope, key)`` function, which only resolves classic workspace-level scopes (a
different, three-generations-older system) and cannot address a UC secret at all. This is a
deliberate, explicit reversal of this framework's earlier classic-scope design: every
``secret_scope``/``secret_key`` config field became ``secret_catalog``/``secret_schema``/
``secret_key``.

Using ``dbutils.secrets.get()`` (rather than any SQL-embedded resolution) also sidesteps a
real, previously-discovered bug independent of which secret system is in play: Databricks'
credential-redaction machinery corrupts the *result* of any query whose text contains a
literal ``secret(...)`` call when executed inside a Lakeflow Declarative Pipeline's
graph-definition context -- see git history / docs/16 for the full writeup. UC secrets have
no SQL-callable resolution function at all, so this class of bug cannot recur here even in
principle.

Requires Databricks Runtime 17.3 LTS+ or serverless environment version 4+ (UC secrets'
minimum supported runtime).
"""

import logging
import re

from pyspark.sql import SparkSession

from flowx.lakeflow_framework.exceptions import SecretResolutionError

logger = logging.getLogger(__name__)

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def assert_safe_identifier(name: str, label: str = "identifier") -> str:
    """Validate that ``name`` is a safe, unquoted SQL identifier / secret catalog-schema-key name.

    Metadata-driven SQL expression assembly (encryption, table/column tag DDL) interpolates
    catalog/schema/table/column/secret names sourced from control-table rows. This closes
    off SQL-injection via a compromised or malformed metadata row before any string is
    spliced into a statement. Deliberately strict (no ``.``/``-``, even though Databricks
    UC secret catalog/schema/key names permit some additional characters) -- this same guard
    is shared with catalog/schema/table/column validation elsewhere.

    Raises
    ------
    ValueError
        If ``name`` is empty or contains characters outside ``[A-Za-z0-9_]``.
    """
    try:
        if not name or not _IDENTIFIER_PATTERN.match(name):
            raise ValueError(f"Unsafe or malformed {label}: {name!r}")
        return name
    except TypeError as exc:
        raise ValueError(f"{label} must be a string, got {type(name).__name__}") from exc


def qualified_secret_label(secret_catalog: str, secret_schema: str, secret_key: str) -> str:
    """Build a validated ``catalog.schema.key`` label, for logging/error messages only."""
    assert_safe_identifier(secret_catalog, "secret_catalog")
    assert_safe_identifier(secret_schema, "secret_schema")
    assert_safe_identifier(secret_key, "secret_key")
    return f"{secret_catalog}.{secret_schema}.{secret_key}"


def _fallback_scope_names(secret_catalog: str, secret_schema: str) -> list:
    """Classic workspace scope names to try when a UC secret cannot be resolved.

    A classic scope has a single flat name, so a three-level UC reference has no canonical
    spelling as one. These are the conventional encodings, most-specific first, and they are
    only ever tried after the UC lookup has already failed.
    """
    return [
        f"{secret_catalog}.{secret_schema}",
        f"{secret_catalog}_{secret_schema}",
        secret_schema,
        secret_catalog,
    ]


def resolve_secret_value(spark: SparkSession, secret_catalog: str, secret_schema: str, secret_key: str) -> str:
    """Resolve the plaintext value of a Unity Catalog secret.

    Resolved entirely outside Spark SQL, via ``dbutils.secrets.get(catalog=, schema=,
    key=)`` (constructed from ``spark`` per Databricks' documented pattern for accessing
    ``dbutils`` from library code, not notebook-top-level code) -- the only supported
    resolution API for UC secrets; there is no SQL-callable equivalent.

    Raises
    ------
    SecretResolutionError
        If the catalog/schema/key is malformed, doesn't exist, or the caller lacks the
        required ``READ SECRET`` grant.
    """
    label = qualified_secret_label(secret_catalog, secret_schema, secret_key)
    try:
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
    except Exception as exc:  # noqa: BLE001
        raise SecretResolutionError(f"Unable to resolve Unity Catalog secret '{label}': {exc}") from exc

    try:
        return dbutils.secrets.get(catalog=secret_catalog, schema=secret_schema, key=secret_key)
    except Exception as uc_exc:  # noqa: BLE001
        # A metastore with UC secrets switched off raises UC_SECRETS_NOT_ENABLED here. That is
        # an environment capability, not a spec error: the same reference is still resolvable
        # from a classic workspace scope, so fall back rather than failing the update.
        #
        # The fallback is deliberately NOT a second spelling of a secret reference -- the spec
        # keeps exactly one shape, `{secret_catalog, secret_schema, secret_key}`. Only the
        # RESOLUTION is widened, and only after the UC lookup has actually failed, so a
        # workspace with UC secrets enabled behaves exactly as before and never consults a
        # scope. See docs/05_security_and_cryptography.md section 4.2.
        for scope in _fallback_scope_names(secret_catalog, secret_schema):
            try:
                value = dbutils.secrets.get(scope=scope, key=secret_key)
            except Exception:  # noqa: BLE001, S112
                continue
            logger.warning(
                "Unity Catalog secret '%s' was not resolvable (%s); resolved it from the "
                "classic workspace scope '%s' instead. This fallback exists for metastores "
                "where UC secrets are not enabled -- enable them and recreate the secret as "
                "%s to remove the ambiguity.",
                label, type(uc_exc).__name__, scope, label,
            )
            return value

        raise SecretResolutionError(
            f"Unable to resolve Unity Catalog secret '{label}': {uc_exc}. No classic workspace "
            f"scope fallback matched either (tried: "
            f"{', '.join(repr(s) for s in _fallback_scope_names(secret_catalog, secret_schema))})."
        ) from uc_exc


def resolve_secret_ref(spark: SparkSession, secret_ref: dict) -> str:
    """Resolve a ``{secret_catalog, secret_schema, secret_key}`` dict, as validated by
    ``onboarding/spec_validator.py::check_secret_ref``.

    Convenience wrapper for the common case of a secret reference threaded through JSON
    config (encrypted_columns, decrypted_columns, PGP keys, sink credentials).

    Raises
    ------
    SecretResolutionError
        Propagated from :func:`resolve_secret_value`.
    """
    return resolve_secret_value(
        spark, secret_ref["secret_catalog"], secret_ref["secret_schema"], secret_ref["secret_key"]
    )
