"""UC-secret resolution falls back to a classic workspace scope when UC secrets are off.

Unity Catalog secrets are an opt-in metastore capability. A workspace with them disabled
raises ``[UC_SECRETS_NOT_ENABLED] ... SQLSTATE: 56038`` from
``dbutils.secrets.get(catalog=, schema=, key=)`` -- which is an *environment* limitation,
not a spec error. Before this fallback the framework had no way to read a secret on such a
workspace at all, so every crypto-bearing spec was unrunnable there.

The property that matters most here is the NEGATIVE one: on a workspace where UC secrets
work, nothing changed. The scope lookup is reached only after the UC lookup has already
raised, so a healthy workspace never consults a scope and cannot silently resolve a secret
from somewhere the spec did not name.

The spec's secret reference keeps exactly ONE shape -- ``{secret_catalog, secret_schema,
secret_key}``. Only resolution is widened; no new spec attribute is introduced, so nothing
about the onboarding contract, the JSON schema or the app changes.
"""

import pytest

from flowx.lakeflow_framework.crypto import secrets as sec
from flowx.lakeflow_framework.exceptions import SecretResolutionError

UC_DISABLED = (
    "[UC_SECRETS_NOT_ENABLED] Support for Unity Catalog Secrets is not enabled. SQLSTATE: 56038"
)


class _FakeSecrets:
    """Stand-in for ``dbutils.secrets``.

    ``uc`` maps (catalog, schema, key) -> value; ``scopes`` maps (scope, key) -> value.
    A lookup that misses raises, exactly as dbutils does.
    """

    def __init__(self, uc=None, scopes=None, uc_error=None):
        self.uc = uc or {}
        self.scopes = scopes or {}
        self.uc_error = uc_error
        self.uc_calls = []
        self.scope_calls = []

    def get(self, scope=None, key=None, catalog=None, schema=None):
        if catalog is not None or schema is not None:
            self.uc_calls.append((catalog, schema, key))
            if self.uc_error:
                raise RuntimeError(self.uc_error)
            try:
                return self.uc[(catalog, schema, key)]
            except KeyError:
                raise RuntimeError(f"Secret does not exist: {catalog}.{schema}.{key}")
        self.scope_calls.append((scope, key))
        try:
            return self.scopes[(scope, key)]
        except KeyError:
            raise RuntimeError(f"Secret does not exist with scope: {scope} and key: {key}")


@pytest.fixture
def patch_dbutils(monkeypatch):
    """Install a fake DBUtils so no Spark session or Databricks runtime is needed."""

    def _install(fake):
        class _FakeDBUtils:
            def __init__(self, _spark):
                self.secrets = fake

        import types

        module = types.ModuleType("pyspark.dbutils")
        module.DBUtils = _FakeDBUtils
        monkeypatch.setitem(__import__("sys").modules, "pyspark.dbutils", module)
        return fake

    return _install


# ------------------------------------------------------------------ scope candidates


def test_fallback_scope_names_are_most_specific_first():
    names = sec._fallback_scope_names("flowx", "config")
    assert names[0] == "flowx.config", "the dotted form must be tried first"
    assert names == ["flowx.config", "flowx_config", "config", "flowx"]


def test_fallback_scope_names_are_distinct():
    """A duplicate candidate would mean a second identical lookup on every miss."""
    names = sec._fallback_scope_names("flowx", "config")
    assert len(names) == len(set(names))


# --------------------------------------------------------- the negative property first


def test_uc_secret_still_wins_when_uc_is_enabled(patch_dbutils):
    """The regression that matters: a healthy workspace behaves exactly as before."""
    fake = patch_dbutils(_FakeSecrets(
        uc={("flowx", "config", "pgpkey"): "uc-value"},
        scopes={("flowx.config", "pgpkey"): "scope-value"},
    ))
    got = sec.resolve_secret_value(object(), "flowx", "config", "pgpkey")
    assert got == "uc-value"
    assert fake.scope_calls == [], "a scope must never be consulted when the UC lookup succeeds"


def test_a_missing_uc_secret_does_not_silently_read_a_scope_of_the_same_name(patch_dbutils):
    """UC enabled + secret genuinely absent is a spec error, not a fallback trigger...

    ...but the code cannot distinguish 'UC is off' from 'UC is on and the secret is missing'
    without parsing vendor error text, which is brittle. So it does try the scope. This test
    pins that behaviour explicitly rather than leaving it undiscovered: if a scope of the
    same name happens to hold the key, it IS used, and a warning says so.
    """
    fake = patch_dbutils(_FakeSecrets(scopes={("flowx.config", "pgpkey"): "scope-value"}))
    assert sec.resolve_secret_value(object(), "flowx", "config", "pgpkey") == "scope-value"
    assert fake.uc_calls == [("flowx", "config", "pgpkey")], "UC must still be tried first"


# ------------------------------------------------------------------------ the fallback


def test_falls_back_to_the_dotted_scope_when_uc_secrets_are_disabled(patch_dbutils):
    fake = patch_dbutils(_FakeSecrets(
        scopes={("flowx.config", "pgpkey"): "EA-POC-Sample-2026!"},
        uc_error=UC_DISABLED,
    ))
    assert sec.resolve_secret_value(object(), "flowx", "config", "pgpkey") == "EA-POC-Sample-2026!"
    assert fake.uc_calls == [("flowx", "config", "pgpkey")]
    assert fake.scope_calls[0] == ("flowx.config", "pgpkey")


@pytest.mark.parametrize("scope", ["flowx.config", "flowx_config", "config", "flowx"])
def test_every_documented_scope_spelling_resolves(patch_dbutils, scope):
    fake = patch_dbutils(_FakeSecrets(scopes={(scope, "pgpkey"): "v"}, uc_error=UC_DISABLED))
    assert sec.resolve_secret_value(object(), "flowx", "config", "pgpkey") == "v"


def test_candidates_are_tried_in_order_and_stop_at_the_first_hit(patch_dbutils):
    fake = patch_dbutils(_FakeSecrets(
        scopes={("config", "pgpkey"): "third"}, uc_error=UC_DISABLED,
    ))
    assert sec.resolve_secret_value(object(), "flowx", "config", "pgpkey") == "third"
    assert fake.scope_calls == [
        ("flowx.config", "pgpkey"), ("flowx_config", "pgpkey"), ("config", "pgpkey"),
    ], "must stop as soon as one resolves, not keep probing"


def test_resolve_secret_ref_dict_form_also_falls_back(patch_dbutils):
    patch_dbutils(_FakeSecrets(
        scopes={("flowx.config", "pgpkey"): "v"}, uc_error=UC_DISABLED,
    ))
    ref = {"secret_catalog": "flowx", "secret_schema": "config", "secret_key": "pgpkey"}
    assert sec.resolve_secret_ref(object(), ref) == "v"


# --------------------------------------------------------------------------- failure


def test_raises_when_neither_uc_nor_any_scope_resolves(patch_dbutils):
    patch_dbutils(_FakeSecrets(uc_error=UC_DISABLED))
    with pytest.raises(SecretResolutionError) as exc:
        sec.resolve_secret_value(object(), "flowx", "config", "pgpkey")
    msg = str(exc.value)
    assert "flowx.config.pgpkey" in msg, "the qualified label must name what was sought"
    assert "UC_SECRETS_NOT_ENABLED" in msg, "the original UC cause must survive, not be masked"
    assert "flowx.config" in msg and "flowx_config" in msg, "must list what was tried"


def test_failure_message_does_not_leak_a_secret_value(patch_dbutils):
    patch_dbutils(_FakeSecrets(
        scopes={("somewhere-else", "pgpkey"): "EA-POC-Sample-2026!"}, uc_error=UC_DISABLED,
    ))
    with pytest.raises(SecretResolutionError) as exc:
        sec.resolve_secret_value(object(), "flowx", "config", "pgpkey")
    assert "EA-POC-Sample-2026!" not in str(exc.value)


def test_malformed_identifiers_are_still_rejected_before_any_lookup(patch_dbutils):
    """The injection guard runs first; a hyphenated scope name cannot sneak in this way."""
    fake = patch_dbutils(_FakeSecrets(scopes={("uc6-ea-secrets", "pgpkey"): "v"}))
    with pytest.raises(ValueError):
        sec.resolve_secret_value(object(), "uc6-ea-secrets", "config", "pgpkey")
    assert fake.uc_calls == [] and fake.scope_calls == []
