"""The five v1.7.4 attributes must exist in the app, not only in the framework.

The failure this guards against is silent and one-directional: the framework gains an
attribute, the builder never offers it, and an author concludes the capability does not
exist. The reverse — the builder offering something onboarding rejects — is already
covered by ``test_framework_spec_agreement.py``.

Three layers are pinned here, because an attribute can be present in one and missing
from the next:

1. the **server registry** (config/registry/*.json) — what the API serves;
2. the **validation rules** (config/validation/rules.json) — what the builder rejects,
   and, just as importantly, what it must NOT reject on a legacy spec;
3. the **shipped frontend bundle** (web/dist) — what the browser actually receives.
   An un-rebuilt dist keeps serving a form without the new fields, which is why
   ``npm run build`` is part of the definition of done.

Every attribute here is additive with a default that reproduces pre-1.7.4 behaviour, so
the legacy-payload cases below assert the far more important property: an existing spec
that never mentions these keys must draw no new errors.
"""

import json

import pytest

from server.core.predicates import evaluate_predicate
from server.core.registry import RegistryManager

Z = "source_config.source_zip_handling"
P = Z + ".pre_extraction_decryption"
SC = "target_config.sink_config"
PE = SC + ".post_export_archive.pgp_encryption"
SFO = SC + ".staged_file_options"
ET = SC + ".export_trigger"

# (path, flow kinds it must be registered for)
NEW_FIELDS = [
    (Z + ".member_format", ("ingestion",)),
    (SFO + ".delimiter", ("ingestion", "transformation")),
    (SFO + ".include_header", ("ingestion", "transformation")),
    (SFO + ".line_terminator", ("ingestion", "transformation")),
    (SC + ".post_export_archive.archive_format", ("ingestion", "transformation")),
    (PE + ".passphrase_secret.secret_key", ("ingestion", "transformation")),
    (ET, ("ingestion", "transformation")),
]

NEW_RULE_IDS = {
    "member_format_enum",
    "gzip_member_rejects_zip_passphrase",
    "pgp_symmetric_requires_passphrase",
    "pgp_symmetric_rejects_private_key",
    "staged_file_options_csv_only",
    "staged_delimiter_single_character",
    "staged_line_terminator_enum",
    "archive_format_enum",
    "pgp_encryption_exactly_one_secret",
    "pgp_encryption_requires_a_secret",
    "pgp_symmetric_egress_rejects_signing",
    "gzip_archive_ignores_zip_password",
    "export_trigger_enum",
    "export_trigger_pgp_zip_only",
}


@pytest.fixture(scope="module")
def reg():
    return RegistryManager()


@pytest.fixture(scope="module")
def rules():
    path = RegistryManager().config_dir / "validation" / "rules.json"
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)["rules"]


# --------------------------------------------------------------------- registry


@pytest.mark.parametrize("path,kinds", NEW_FIELDS)
def test_new_field_is_registered(reg, path, kinds):
    for kind in kinds:
        assert reg.get_field_definition(kind, path) is not None, (
            f"{path} missing from the {kind} registry — the builder cannot offer it"
        )


@pytest.mark.parametrize("path,kinds", NEW_FIELDS)
def test_new_field_carries_no_default(reg, path, kinds):
    """Same contract as every other field: absent means absent. A default here would
    re-inject a value the user never chose and silently change an export's shape."""
    for kind in kinds:
        f = reg.get_field_definition(kind, path)
        assert "default" not in f, f"{path} ({kind}) carries a default"


def test_pgp_symmetric_is_offered_as_a_decryption_type(reg):
    f = reg.get_field_definition("ingestion", P + ".type")
    assert f is not None
    assert "pgp_symmetric" in f["options"], (
        "pgp_symmetric is not selectable — symmetric ingest is unreachable from the builder"
    )
    assert "pgp" in f["options"], "the asymmetric type must remain available"


@pytest.mark.parametrize("path,enum", [
    (Z + ".member_format", {"zip", "gzip"}),
    (SC + ".post_export_archive.archive_format", {"zip", "gzip"}),
    (SFO + ".line_terminator", {"crlf", "lf"}),
    (ET, {"per_micro_batch", "per_update"}),
])
def test_enum_fields_offer_exactly_the_framework_values(reg, path, enum):
    kind = "ingestion"
    f = reg.get_field_definition(kind, path)
    offered = {o for o in f["options"] if o}
    assert offered == enum, f"{path} offers {offered}, framework allows {enum}"


@pytest.mark.parametrize("path,kinds", NEW_FIELDS)
def test_new_field_is_conditionally_visible(reg, path, kinds):
    """None of these apply unconditionally — an always-visible field would put ZIP
    options on a Kafka sink."""
    for kind in kinds:
        f = reg.get_field_definition(kind, path)
        assert f.get("visible_when"), f"{path} ({kind}) has no visible_when guard"


# ---------------------------------------------------------------------- rules


def test_all_new_rules_are_present(rules):
    have = {r["id"] for r in rules}
    assert NEW_RULE_IDS <= have, f"missing rules: {sorted(NEW_RULE_IDS - have)}"


def _fire(rules, ctx):
    return {r["id"] for r in rules
            if r["id"] in NEW_RULE_IDS and evaluate_predicate(r["when"], ctx)}


@pytest.mark.parametrize("name,ctx", [
    ("empty flow", {}),
    ("legacy asymmetric zip ingest", {
        Z + ".enabled": True,
        P + ".type": "pgp",
        P + ".private_key_secret.secret_key": "k",
        P + ".secret_passphrase.secret_key": "zp",
    }),
    ("legacy asymmetric zip egress", {
        SC + ".format": "pgp_zip",
        SC + ".staged_file_format": "csv",
        SC + ".post_export_archive.enabled": True,
        SC + ".post_export_archive.secret.secret_key": "zp",
        PE + ".enabled": True,
        PE + ".recipient_public_key_secret.secret_key": "k",
        PE + ".sign_with_private_key_secret.secret_key": "s",
    }),
    ("uc6 gzip + symmetric ingest", {
        Z + ".enabled": True,
        Z + ".member_format": "gzip",
        P + ".type": "pgp_symmetric",
        P + ".passphrase_secret.secret_key": "pgpkey",
    }),
    ("v1.7.5 per_update export", {
        SC + ".format": "pgp_zip",
        SC + ".staged_file_format": "csv",
        ET: "per_update",
        SC + ".post_export_archive.enabled": True,
    }),
    ("v1.7.5 explicit per_micro_batch", {
        SC + ".format": "pgp_zip",
        ET: "per_micro_batch",
    }),
    ("uc6 gzip + symmetric egress", {
        SC + ".format": "pgp_zip",
        SC + ".staged_file_format": "csv",
        SFO + ".delimiter": "|",
        SFO + ".include_header": True,
        SFO + ".line_terminator": "lf",
        SC + ".post_export_archive.enabled": True,
        SC + ".post_export_archive.archive_format": "gzip",
        PE + ".enabled": True,
        PE + ".passphrase_secret.secret_key": "pgpkey",
    }),
])
def test_valid_configurations_draw_no_new_errors(rules, name, ctx):
    """The regression that matters: a spec written before v1.7.4 — and a correct one
    written after — must not light up a single new rule."""
    fired = _fire(rules, ctx)
    assert not fired, f"{name} wrongly triggered {sorted(fired)}"


@pytest.mark.parametrize("expected,ctx", [
    ("member_format_enum", {Z + ".enabled": True, Z + ".member_format": "tar"}),
    ("gzip_member_rejects_zip_passphrase", {
        Z + ".enabled": True, Z + ".member_format": "gzip",
        P + ".secret_passphrase.secret_key": "x"}),
    ("pgp_symmetric_requires_passphrase", {
        Z + ".enabled": True, P + ".type": "pgp_symmetric"}),
    ("pgp_symmetric_rejects_private_key", {
        Z + ".enabled": True, P + ".type": "pgp_symmetric",
        P + ".passphrase_secret.secret_key": "p",
        P + ".private_key_secret.secret_key": "k"}),
    ("staged_file_options_csv_only", {
        SC + ".staged_file_format": "json", SFO + ".delimiter": "|"}),
    ("staged_delimiter_single_character", {
        SC + ".staged_file_format": "csv", SFO + ".delimiter": "||"}),
    ("staged_line_terminator_enum", {
        SC + ".staged_file_format": "csv", SFO + ".line_terminator": "cr"}),
    ("archive_format_enum", {SC + ".post_export_archive.archive_format": "tar"}),
    ("pgp_encryption_exactly_one_secret", {
        PE + ".enabled": True,
        PE + ".passphrase_secret.secret_key": "p",
        PE + ".recipient_public_key_secret.secret_key": "k"}),
    ("pgp_encryption_requires_a_secret", {PE + ".enabled": True}),
    ("pgp_symmetric_egress_rejects_signing", {
        PE + ".enabled": True,
        PE + ".passphrase_secret.secret_key": "p",
        PE + ".sign_with_private_key_secret.secret_key": "s"}),
    ("export_trigger_enum", {SC + ".format": "pgp_zip", ET: "hourly"}),
    ("export_trigger_pgp_zip_only", {SC + ".format": "delta", ET: "per_update"}),
    ("gzip_archive_ignores_zip_password", {
        SC + ".post_export_archive.archive_format": "gzip",
        SC + ".post_export_archive.secret.secret_key": "zp"}),
])
def test_each_violation_fires_exactly_its_own_rule(rules, expected, ctx):
    """Precision matters as much as coverage: a rule that fires alongside three others
    buries the actionable message."""
    assert _fire(rules, ctx) == {expected}


def test_the_two_pgp_secrets_are_mutually_exclusive_not_merely_optional(rules):
    """Neither-set and both-set are both errors. Only exactly-one passes."""
    both = {PE + ".enabled": True,
            PE + ".passphrase_secret.secret_key": "p",
            PE + ".recipient_public_key_secret.secret_key": "k"}
    neither = {PE + ".enabled": True}
    assert _fire(rules, both), "both secrets set must be rejected"
    assert _fire(rules, neither), "neither secret set must be rejected"
    for only in (PE + ".passphrase_secret.secret_key",
                 PE + ".recipient_public_key_secret.secret_key"):
        assert not _fire(rules, {PE + ".enabled": True, only: "x"}), (
            f"exactly one secret ({only}) must be accepted"
        )


# ------------------------------------------------------------ shipped frontend


@pytest.mark.parametrize("token", [
    "member_format",
    "pgp_symmetric",
    "staged_file_options",
    "archive_format",
    "export_trigger",
])
def test_new_attributes_reach_the_shipped_bundle(frontend_text, token):
    """Databricks Apps does not build at deploy time. If web/dist was not rebuilt after
    registry.js changed, the browser keeps receiving the old form — the field exists
    everywhere except where an author would use it."""
    assert token in frontend_text, (
        f"{token} is absent from the shipped bundle -- run `npm run build` in "
        f"databricks-app/web after changing registry.js"
    )
