"""The central branding layer is the single source of truth for the brand (v1.7.14).

The rebrand FlowX -> Metaflow / hoonartek -> NRM Analytix moved every user-visible string
and every deployed object name into ``branding/branding.json``. These tests hold that
arrangement in place from both directions:

* the layer itself works -- the config parses, carries every key a consumer needs, and the
  loader derives prefixed environment variable names from it;
* the rename did not leak and did not overreach. A leak is an old brand token still sitting
  in live source, which is how a half-done rename hides (see ``test_no_stale_brand_tokens``).
  Overreach is the opposite and worse failure: the Python package ``flowx`` and the Unity
  Catalog of the same name are *runtime identity*, not branding, and renaming either breaks
  every import or orphans a live catalog. ``test_python_package_identity_is_unchanged``
  fails if a future sweep touches them.

Everything here is offline: no Spark, no workspace, no network.
"""

from __future__ import annotations

import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

# tests/unit/test_branding.py -> repo root. Resolved from __file__ rather than the process
# working directory, so the suite passes whether pytest is invoked from the repo root, from
# tests/, or from an IDE with some other cwd. ``resolve()`` also normalises the Windows
# short-path / drive-letter-case differences that make plain string comparison unreliable.
REPO_ROOT = Path(__file__).resolve().parents[2]
BRANDING_DIR = REPO_ROOT / "branding"
BRANDING_JSON = BRANDING_DIR / "branding.json"
BRANDING_PY = BRANDING_DIR / "branding.py"


def _load_branding_module():
    """Import ``branding/branding.py`` by path.

    The repo root is not guaranteed to be on ``sys.path`` (only ``src`` is, via the editable
    install), and ``branding`` is deliberately not a distributed package -- the Databricks App
    gets a generated copy instead, because only ``databricks-app/`` is uploaded to Apps. Load
    it from its file location so this test does not depend on either detail.
    """
    spec = importlib.util.spec_from_file_location("_branding_under_test", BRANDING_PY)
    assert spec and spec.loader, f"cannot build an import spec for {BRANDING_PY}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------------------------------
# 1. branding.json exists, parses, and is complete
# --------------------------------------------------------------------------------------

#: Dotted paths every consumer reads. A missing one is not cosmetic: the generator writes
#: ``branding_generated.py`` and ``branding.js`` from these, so an absent key becomes an
#: ``AttributeError`` inside the deployed app rather than a failure here.
REQUIRED_KEYS = [
    "framework.display_name",
    "framework.slug",
    "vendor.display_name",
    "app.title",
    "app.deployed_name",
    "env.prefix",
    "docs.site_name",
    "docs.copyright",
    "consoles.observability_dashboard_title",
    "consoles.genie_space_title",
    "logo.light.published",
    "logo.dark.published",
    "logo.alt_text",
]


@pytest.fixture(scope="module")
def branding() -> dict:
    assert BRANDING_JSON.is_file(), (
        f"{BRANDING_JSON} is missing. It is the single source of truth for the brand; "
        "scripts/apply_branding.py reads it to regenerate every derived artifact."
    )
    return json.loads(BRANDING_JSON.read_text(encoding="utf-8"))


def _dig(data: dict, dotted: str):
    node = data
    for part in dotted.split("."):
        assert isinstance(node, dict) and part in node, f"branding.json has no key {dotted!r}"
        node = node[part]
    return node


@pytest.mark.parametrize("dotted", REQUIRED_KEYS)
def test_branding_json_has_every_required_key(branding, dotted):
    value = _dig(branding, dotted)
    assert isinstance(value, str) and value.strip(), f"branding.json {dotted} must be a non-empty string"


def test_branding_json_carries_the_decided_brand(branding):
    """The values themselves, not merely their presence.

    A config that parses but still says "FlowX" would satisfy every structural check above
    while leaving the rebrand undone.
    """
    assert branding["framework"]["display_name"] == "Metaflow"
    assert branding["framework"]["slug"] == "metaflow"
    assert branding["vendor"]["display_name"] == "NRM Analytix"
    assert branding["app"]["title"] == "Metaflow Onboarding"
    assert branding["app"]["deployed_name"] == "metaflow-onboarding"
    assert branding["env"]["prefix"] == "METAFLOW"


# --------------------------------------------------------------------------------------
# 2. branding.py loads it and derives env var names
# --------------------------------------------------------------------------------------


def test_branding_module_loads_the_config():
    mod = _load_branding_module()
    data = mod.load_branding()
    assert isinstance(data, dict)
    assert mod.framework_name() == "Metaflow"
    assert mod.vendor_name() == "NRM Analytix"
    assert mod.app_title() == "Metaflow Onboarding"
    assert mod.app_deployed_name() == "metaflow-onboarding"


def test_env_var_derives_the_prefix_from_config():
    """``env_var`` is what the rename hangs on -- the app derives every variable name
    through it rather than pasting ``METAFLOW_`` literals, so the prefix moves in one edit."""
    mod = _load_branding_module()
    assert mod.env_var("SPEC_CATALOG") == "METAFLOW_SPEC_CATALOG"
    assert mod.env_var("FAKE_DBX") == "METAFLOW_FAKE_DBX"
    # Idempotent: applying it to an already-prefixed name must not double the prefix, or a
    # reader and a writer that disagree about whether to pre-prefix silently diverge.
    assert mod.env_var("METAFLOW_SPEC_CATALOG") == "METAFLOW_SPEC_CATALOG"


def test_env_var_rejects_an_empty_name():
    mod = _load_branding_module()
    with pytest.raises(mod.BrandingError):
        mod.env_var("   ")


def test_get_raises_on_an_unknown_key():
    """A typo must fail loudly rather than render ``None`` into a deployed title."""
    mod = _load_branding_module()
    with pytest.raises(mod.BrandingError):
        mod.get("framework.no_such_key")


# --------------------------------------------------------------------------------------
# 3. the app's served title agrees with branding.json
# --------------------------------------------------------------------------------------


def test_app_config_does_not_shadow_the_branding_title():
    """``databricks-app/config/index.json`` must NOT carry ``app.title``.

    ``AppInfo.title`` defaults to ``APP_TITLE`` from the generated branding module, but a
    Pydantic default is only reached when the key is ABSENT. While index.json supplied a
    title of its own it always won, so ``branding.json`` could say one thing and the app
    serve another with nothing failing -- the generated value was dead.
    ``load_settings()`` now pops the key defensively; this asserts the config file does not
    reintroduce it, because a reintroduced key is silent rather than loud.
    """
    index = REPO_ROOT / "databricks-app" / "config" / "index.json"
    assert index.is_file(), f"{index} is missing"
    app = json.loads(index.read_text(encoding="utf-8")).get("app", {})
    assert "title" not in app, (
        "databricks-app/config/index.json reintroduced app.title. The brand is owned by "
        "branding/branding.json and reaches the app through server/branding_generated.py; "
        "a title here shadows it silently. Remove the key."
    )


def test_served_app_title_comes_from_branding(branding):
    """End to end: the title the app actually serves must be the configured brand.

    This is the propagation assertion the arrangement hangs on. Loading real settings
    catches the failure mode a file-content check cannot -- that some other override, or a
    returning index.json key, shadows the branding value at runtime.
    """
    app_dir = REPO_ROOT / "databricks-app"
    sys.path.insert(0, str(app_dir))
    try:
        for name in [m for m in sys.modules if m.startswith("server.")]:
            del sys.modules[name]
        from server.settings import load_settings

        served = load_settings(app_dir / "config" / "index.json").app.title
    finally:
        sys.path.remove(str(app_dir))
    assert served == branding["app"]["title"], (
        f"the app serves {served!r} but branding.json app.title is "
        f"{branding['app']['title']!r}. Something is shadowing the branding value."
    )


def test_generated_app_branding_matches_branding(branding):
    """The generated app-side copy must not have drifted from its source.

    The app cannot import ``branding/branding.py`` -- Databricks Apps uploads only
    ``databricks-app/`` -- so ``scripts/apply_branding.py`` embeds the values into
    ``server/branding_generated.py``. A stale copy deploys the old brand while the repo
    says otherwise; regenerate with ``python scripts/apply_branding.py``.
    """
    generated = REPO_ROOT / "databricks-app" / "server" / "branding_generated.py"
    assert generated.is_file(), f"{generated} is missing; run python scripts/apply_branding.py"

    spec = importlib.util.spec_from_file_location("_branding_generated_under_test", generated)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.APP_TITLE == branding["app"]["title"]
    assert module.ENV_PREFIX == branding["env"]["prefix"]
    assert module.env_var("SPEC_CATALOG") == "METAFLOW_SPEC_CATALOG"


# --------------------------------------------------------------------------------------
# 4. no stale brand token survives in live source
# --------------------------------------------------------------------------------------

#: Paths whose old brand tokens are legitimate and must never be rewritten.
#:
#: The first group is *history*: release notes, enhancement logs, dev logs and archives
#: record what was true at the time, including verbatim platform error messages naming the
#: old app id. Rewriting them makes the record describe a system that never existed.
#: The second group is *generated output* -- regenerated wholesale, never hand-edited.
ALLOWED_HISTORICAL = (
    "RELEASE_NOTES.md",
    "enhancement_logs/",
    "docs/archive/",
    "archive/",
    "site/",
    "databricks-app/docs_site/",
    "databricks-app/dev_logs/",
    ".databricks/",
    "__pycache__/",
    "dist/",
    "databricks-app/web/dist/",
    "node_modules/",
    ".git/",
    # This test names the tokens it searches for, so it would always match itself.
    "tests/unit/test_branding.py",
)

#: The agent-skill directory ``.claude/skills/flowx-onboarding/`` is NOT renamed by this
#: rebrand, on purpose. Its basename is the skill's invocation name (``/flowx-onboarding``)
#: and it is referenced by string from the SKILL.md frontmatter, scripts/sync_agent_skill.py,
#: tests/unit/test_agent_skill_layout.py, .pre-commit-config.yaml, .github/workflows/docs.yml
#: and several docs pages. Renaming it is a user-facing change that has to move all of those
#: in one commit; until that is done deliberately, every mention of the path is correct and
#: must not be flagged. Tracked as a follow-up, not a leak.
SKILL_DIR_NAME = "flowx-onboarding"

#: Lines that *describe* the old names rather than use them. The rename is explained in
#: prose in several places -- ``settings.py`` documents that there is deliberately no
#: fallback to the legacy prefix, and branding/README.md walks through the migration. A
#: guard that cannot tell explanation from usage would force those comments to be deleted,
#: which would remove exactly the warning that stops someone reintroducing the fallback.
EXPLANATORY = re.compile(r"legacy FLOWX_|FLOWX_\*|`FLOWX_")

#: Leaks that exist today, are known, and belong to surfaces this change does not own.
#: Enumerated file-by-file rather than pattern-matched so the guard still fails the moment a
#: *new* leak appears anywhere, including a new one in these same files. Each entry is a real
#: defect with a real consequence, not an accepted spelling:
#:
#: * ``FLOWX_DOCS_SKIP_REGEN`` -- reader (scripts/mkdocs_hooks.py) and writer
#:   (.github/workflows/docs.yml) still agree, so the docs build works; it is the one
#:   ``FLOWX_*`` variable left, and both halves must move in a single commit or CI step 4b
#:   silently starts regenerating the reference tree it was told to trust.
#: * The three docs pages and databricks-app/README.md tell a developer to export
#:   ``FLOWX_FAKE_DBX``. The app now reads ``METAFLOW_FAKE_DBX``, so following those
#:   instructions yields no mock and the suite attempts real workspace calls -- the
#:   instruction is not merely stale, it is wrong.
#: * docs/v1.5.01_json_attribute_delta.json records a historical delta but is not under the
#:   allowed-historical paths.
KNOWN_OUTSTANDING = {
    ".github/workflows/docs.yml",
    "scripts/mkdocs_hooks.py",
    "docs/console/spec_builder.md",
    "docs/onboarding/03_spec_builder_app.md",
    "databricks-app/README.md",
    "docs/v1.5.01_json_attribute_delta.json",
}

#: The deployed Databricks App name and the old environment-variable prefix. Both are
#: *deployed object names* that moved with the rebrand, so a survivor in live source is a
#: half-done rename. ``FLOWX_`` is the dangerous one: the bundle also writes unbranded
#: aliases (``ONBOARDING_JOB_ID``, ``DATABRICKS_ONBOARDING_JOB_ID``) and settings falls back
#: to ``DATABRICKS_HOST``, so a missed reader keeps working through an alias and the defect
#: stays invisible until a deploy without them.
#:
#: ``FLOWX_`` is matched only for the environment variables that actually exist, rather than
#: as a bare prefix. A bare prefix also matches data that merely looks like one -- UC3's
#: synthetic Debezium envelope carries a fabricated ``'user_name', 'FLOWX_RECONCILIATION'``
#: inside its CDC payload, which is test *data*, not configuration, and renaming it would
#: change the fixture the reconciliation tests assert against.
STALE_TOKENS = re.compile(
    r"flowx-onboarding"
    r"|FLOWX_(?:APP_CONFIG|LOG_LEVEL|WORKSPACE_HOST|ONBOARDING_JOB_ID|VALIDATE_JOB_ID"
    r"|SPEC_CATALOG|SPEC_ENV|SPEC_VOLUME_ROOT|SPEC_WORKSPACE_ROOT|FAKE_DBX"
    r"|TEST_OBO_STRICT|DOCS_SKIP_REGEN)"
)


def _tracked_files() -> list[str]:
    """Every git-tracked path, so untracked scratch files and ignored build output never
    fail the suite. ``-z`` avoids git's quoting of non-ASCII names."""
    out = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=REPO_ROOT,
        capture_output=True,
        check=True,
    ).stdout
    return [p.decode("utf-8") for p in out.split(b"\x00") if p]


def _is_allowed(path: str) -> bool:
    # git reports forward slashes on every platform, so this comparison is Windows-safe.
    return any(part in path for part in ALLOWED_HISTORICAL)


def _stale_hits_in(text: str) -> list[str]:
    """Return the lines that genuinely *use* an old brand token.

    Scanned line by line rather than whole-file so the two legitimate cases can be told
    apart from a real leak: a mention of the deliberately-unrenamed agent-skill directory,
    and prose that explains the old name instead of using it.
    """
    hits = []
    for line in text.splitlines():
        if not STALE_TOKENS.search(line):
            continue
        if EXPLANATORY.search(line):
            continue
        # Drop the skill-directory path, then re-test: a line mentioning that path AND a
        # real stale token still counts.
        if not STALE_TOKENS.search(line.replace(SKILL_DIR_NAME, "")):
            continue
        hits.append(line.strip())
    return hits


def _all_offenders() -> dict[str, list[str]]:
    offenders: dict[str, list[str]] = {}
    for rel in _tracked_files():
        if _is_allowed(rel):
            continue
        full = REPO_ROOT / rel
        try:
            raw = full.read_bytes()
        except (OSError, FileNotFoundError):
            # A path git still tracks but that is absent from the working tree (mid-rename,
            # or a sparse checkout) is not this test's business.
            continue
        hits = _stale_hits_in(raw.decode("utf-8", errors="ignore"))
        if hits:
            offenders[rel] = hits
    return offenders


def _format(offenders: dict[str, list[str]]) -> str:
    return "\n".join(
        f"  {path}\n" + "\n".join(f"      {line[:120]}" for line in lines)
        for path, lines in sorted(offenders.items())
    )


def test_no_stale_brand_tokens():
    """No file outside the allowed-historical and known-outstanding lists may carry an old
    brand token. This is the guard that catches a half-done rename, which is otherwise
    invisible: the bundle writes unbranded aliases and settings falls back to
    ``DATABRICKS_HOST``, so a missed reader keeps the app working and hides the defect."""
    new = {p: lines for p, lines in _all_offenders().items() if p not in KNOWN_OUTSTANDING}
    assert not new, (
        "NEW stale brand tokens ('flowx-onboarding' / 'FLOWX_*') in live source:\n"
        + _format(new)
    )


def test_known_outstanding_leaks_are_still_accurate():
    """The known-outstanding list must shrink, never rot.

    If one of those files is cleaned up and the entry is left behind, this fails and the
    entry gets removed -- so the exception list cannot quietly become a place where leaks
    are permanently parked.
    """
    offenders = _all_offenders()
    fixed = sorted(KNOWN_OUTSTANDING - set(offenders))
    assert not fixed, (
        "these files no longer carry a stale brand token -- remove them from "
        f"KNOWN_OUTSTANDING in {Path(__file__).name}: {fixed}"
    )


# --------------------------------------------------------------------------------------
# 5. the guard against overreach
# --------------------------------------------------------------------------------------


def test_python_package_identity_is_unchanged():
    """``flowx`` the *package* is runtime identity and is deliberately NOT rebranded.

    It backs ~394 ``from flowx ...`` statements plus ``mock.patch`` target strings and
    ``__module__`` assertions that fail with ``ModuleNotFoundError`` rather than a text
    mismatch. This test is the tripwire: if a future branding sweep renames the package,
    it fails here with a clear reason instead of a storm of import errors.
    """
    import flowx  # noqa: F401  -- importability is the assertion
    from flowx.lakeflow_framework.onboarding import spec_validator  # noqa: F401

    assert flowx.__name__ == "flowx"


def test_pyproject_distribution_name_is_unchanged():
    """The wheel name every deployed job and pipeline installs."""
    pyproject = REPO_ROOT / "pyproject.toml"
    text = pyproject.read_text(encoding="utf-8")
    assert re.search(r'^name\s*=\s*"flowx"\s*$', text, re.M), (
        "pyproject.toml name is no longer \"flowx\". The distribution name is runtime "
        "identity, not branding -- renaming it changes the wheel every job installs."
    )


def test_branding_config_declares_no_runtime_identity_keys(branding):
    """branding.json must not grow a knob for something that is not branding.

    A ``catalog`` or ``package`` key would invite someone to "rebrand" the live Unity
    Catalog or the Python package from config, which orphans control tables or breaks every
    import. The omission is deliberate and is documented in branding/README.md.
    """
    flat = json.dumps(branding).lower()
    for forbidden in ('"catalog"', '"package_name"', '"schema"', '"bundle_name"', '"target"'):
        assert forbidden not in flat, (
            f"branding.json declares {forbidden}, which is runtime identity, not branding. "
            "See the 'What is deliberately NOT configurable' section of branding/README.md."
        )


def test_branding_readme_documents_the_hand_edited_surfaces():
    """The README is the contract for what the generator does *not* do.

    ``apply_branding.py`` writes generated files only; every other branded surface is a
    deliberate hand edit. If that list goes missing, a customer rebranding the repo silently
    ships half a rename.
    """
    readme = BRANDING_DIR / "README.md"
    assert readme.is_file(), f"{readme} is missing"
    text = readme.read_text(encoding="utf-8")
    for expected in ("mkdocs.yml", "agent_skills/", "sync_agent_skill.py", "npm run build"):
        assert expected in text, f"branding/README.md no longer lists {expected!r} as a hand-edited surface"
# --------------------------------------------------------------------------------------
# 6. propagation: every hand-edited surface must agree with branding.json
# --------------------------------------------------------------------------------------
#
# ``apply_branding.py`` writes five generated artifacts. Every OTHER branded surface is a
# hand edit -- static YAML, static HTML and bundle resources that cannot read a config file
# at runtime. That is a deliberate design (a generator that only writes whole generated
# files cannot corrupt hand-written code), but on its own it leaves the claim
# "branding.json is the single source of truth" unenforced: a customer can edit the config,
# run the generator, and every console title, docs heading and <title> silently keeps the
# old brand.
#
# These tests close that hole from the other direction. They do not make the surfaces
# generated; they make a DISAGREEMENT fail the build. Combined with
# ``test_every_branding_key_has_a_destination`` below, no key in branding.json can be
# decorative: it is either read by a generated accessor, or asserted against the file that
# hand-carries it, or explicitly recorded as unused with a reason.


def _read(rel: str) -> str:
    path = REPO_ROOT / rel
    assert path.is_file(), f"{rel} is missing; branding/README.md lists it as a hand-edited surface"
    return path.read_text(encoding="utf-8", errors="ignore")


def test_mkdocs_site_name_matches_branding(branding):
    """``mkdocs.yml`` hand-carries the docs brand; nothing reads ``docs.site_name``."""
    text = _read("mkdocs.yml")
    assert f"site_name: {branding['docs']['site_name']}" in text, (
        "mkdocs.yml site_name disagrees with branding.json docs.site_name. mkdocs.yml is "
        "hand-maintained (branding/README.md lists it); update it in the same commit."
    )
    assert f"site_author: {branding['vendor']['display_name']}" in text, (
        "mkdocs.yml site_author disagrees with branding.json vendor.display_name."
    )
    assert f"copyright: {branding['docs']['copyright']}" in text, (
        "mkdocs.yml copyright disagrees with branding.json docs.copyright."
    )


def test_web_html_titles_match_branding(branding):
    """Static HTML parses before any module loads, so it cannot read ``branding.js``.

    ``index.html`` is what the built app serves; ``preview.html`` is the no-build harness.
    """
    title = branding["app"]["title"]
    assert f"<title>{title}</title>" in _read("databricks-app/web/index.html"), (
        f"databricks-app/web/index.html <title> is not {title!r} (branding.json app.title). "
        "Static HTML cannot read branding.js -- edit it by hand, then `npm run build`."
    )
    assert f"<title>{title}" in _read("databricks-app/web/preview.html"), (
        f"databricks-app/web/preview.html <title> does not start with {title!r}."
    )


def test_app_yaml_env_names_match_the_configured_prefix(branding):
    """``databricks-app/app.yaml`` is read before any Python runs, so it hardcodes the names.

    It governs local ``uvicorn`` runs (the bundle ``config:`` block supersedes it at deploy
    time), which is exactly why a mismatch here is easy to miss: the deployed app is fine
    while local development silently loses its configuration.
    """
    prefix = branding["env"]["prefix"]
    text = _read("databricks-app/app.yaml")
    for suffix in ("APP_CONFIG", "LOG_LEVEL"):
        assert f"name: {prefix}_{suffix}" in text, (
            f"databricks-app/app.yaml does not declare {prefix}_{suffix}. Readers derive "
            "their names from branding.json via env_var(); this writer is hand-maintained "
            "and must move in the same commit."
        )


def test_bundle_app_resource_matches_branding(branding):
    """The deployed Databricks App name and its eight ``<PREFIX>_*`` writers.

    The env-var trap: the bundle also writes the unbranded aliases ONBOARDING_JOB_ID and
    DATABRICKS_ONBOARDING_JOB_ID, so a writer left on the old prefix still *looks* like it
    works. This asserts the names directly rather than trusting behaviour.
    """
    prefix = branding["env"]["prefix"]
    text = _read("resources/flowx_app/flowx_onboarding_app.yml")
    assert f'name: "{branding["app"]["deployed_name"]}"' in text, (
        "the bundle app resource name disagrees with branding.json app.deployed_name."
    )
    for suffix in (
        "APP_CONFIG", "LOG_LEVEL", "WORKSPACE_HOST", "ONBOARDING_JOB_ID",
        "SPEC_CATALOG", "SPEC_ENV", "SPEC_VOLUME_ROOT", "SPEC_WORKSPACE_ROOT",
    ):
        assert f"name: {prefix}_{suffix}" in text, (
            f"resources/flowx_app/flowx_onboarding_app.yml does not write {prefix}_{suffix}; "
            "the app would fall back to the checked-in config defaults."
        )


def test_console_titles_match_branding(branding):
    """The dashboard and Genie titles live in bundle YAML, which cannot read a config file.

    Without this, ``consoles.*`` is the worst kind of config: it *looks* authoritative, a
    customer edits it, and nothing happens with no error.
    """
    consoles = branding["consoles"]
    control = _read("resources/flowx_bi/flowx_control_dashboard.yml")
    assert consoles["control_dashboard_title"] in control, (
        "resources/flowx_bi/flowx_control_dashboard.yml display_name does not carry "
        f"branding.json consoles.control_dashboard_title ({consoles['control_dashboard_title']!r})."
    )
    obs = _read("resources/flowx_bi/flowx_observability_dashboard.yml")
    assert consoles["observability_dashboard_title"] in obs, (
        "resources/flowx_bi/flowx_observability_dashboard.yml display_name does not carry "
        "branding.json consoles.observability_dashboard_title."
    )
    genie = _read("resources/flowx_genie/flowx_observability_genie_space.yml")
    assert f'title: "{consoles["genie_space_title"]}"' in genie, (
        "the Genie space title disagrees with branding.json consoles.genie_space_title."
    )


# --------------------------------------------------------------------------------------
# 7. no key in branding.json may be decorative
# --------------------------------------------------------------------------------------

#: Every dotted key in branding.json, mapped to how it reaches a surface. This is the
#: anti-dead-config gate: a key with no destination is a trap, because a customer edits it,
#: sees no error, and ships the old brand.
#:
#: ``generated`` -- read by apply_branding.py into branding.js / branding_generated.py.
#: ``asserted``  -- hand-carried by a file, held in agreement by a test in section 6.
#: ``unused``    -- deliberately carried for documentation only, with the reason given.
KEY_DESTINATIONS = {
    "framework.display_name": "generated",   # branding.js frameworkName -> Shell.jsx wordmark
    "framework.slug": "generated",           # branding_generated.FRAMEWORK_SLUG
    "framework.tagline": "generated",        # branding.js tagline
    "framework.builder_subtitle": "generated",  # branding.js -> Shell.jsx subtitle
    "vendor.display_name": "asserted",       # branding.js vendorName + mkdocs site_author
    "vendor.slug": "unused",                 # reserved: no surface takes a vendor slug today
    "app.title": "generated",                # branding_generated.APP_TITLE -> served settings
    "app.deployed_name": "asserted",         # bundle app resource name:
    "app.description": "unused",             # the bundle description is reflowed prose, not a whole value
    "app.running_message": "generated",      # branding_generated.APP_RUNNING_MESSAGE -> /health
    "env.prefix": "generated",               # env_var() in both generated accessors
    "docs.site_name": "asserted",            # mkdocs.yml site_name
    "docs.site_description": "unused",       # mkdocs site_description names no product
    "docs.copyright": "asserted",            # mkdocs.yml copyright
    "docs.external_base_url": "generated",   # branding_generated.DOCS_EXTERNAL_BASE_URL
    "consoles.dashboard_prefix": "unused",   # reserved: titles are asserted whole, not prefixed
    "consoles.control_dashboard_title": "asserted",
    "consoles.observability_dashboard_title": "asserted",
    "consoles.genie_space_title": "asserted",
    "consoles.genie_space_description": "unused",  # the YAML prose is reflowed, not a whole value
    "logo.alt_text": "generated",            # branding.js logoAlt
    "logo.light.published": "generated",     # emitted asset filename
    "logo.dark.published": "generated",      # emitted asset filename
}


def _leaf_keys(node, prefix=""):
    """Dotted paths of every string leaf, skipping ``$``-prefixed comments and the
    logo-processing knobs (source filenames, crop flags, tolerances, notes), which are
    generator INPUTS rather than brand strings shown to anyone."""
    skip_leaves = {
        "source_dir", "source", "background", "note", "crop_to_content",
        "background_to_alpha", "alpha_tolerance", "favicon", "favicon_note",
        "schema_version",
    }
    out = {}
    for key, value in node.items():
        if key.startswith("$") or key in skip_leaves:
            continue
        dotted = f"{prefix}{key}"
        if isinstance(value, dict):
            out.update(_leaf_keys(value, dotted + "."))
        elif isinstance(value, str):
            out[dotted] = value
    return out


def test_every_branding_key_has_a_destination(branding):
    """A key nobody reads is a trap, not configuration.

    If you add a key to branding.json, give it a destination here: wire it into
    ``apply_branding.py`` (``generated``), assert the file that hand-carries it in section 6
    (``asserted``), or record it as ``unused`` with the reason. Otherwise a customer edits
    it, gets no effect and no error, and ships a half-rebranded product.
    """
    found = set(_leaf_keys(branding))
    declared = set(KEY_DESTINATIONS)
    undeclared = sorted(found - declared)
    assert not undeclared, (
        "branding.json keys with no declared destination: " + ", ".join(undeclared)
        + ". Add each to KEY_DESTINATIONS in this file as 'generated', 'asserted' or "
        "'unused' (with a reason), so no key can be silently decorative."
    )
    stale = sorted(declared - found)
    assert not stale, (
        "KEY_DESTINATIONS names keys that branding.json no longer has: " + ", ".join(stale)
    )


def test_branding_readme_lists_every_asserted_surface():
    """The README's hand-edit list is the customer-facing half of the same contract.

    Section 6 asserts these surfaces agree with the config; the README is where a customer
    learns they must be edited at all. If a surface is asserted but unlisted, the customer
    finds out by watching CI fail instead of by reading the instructions.
    """
    text = (BRANDING_DIR / "README.md").read_text(encoding="utf-8")
    for rel in (
        "mkdocs.yml",
        "databricks-app/web/index.html",
        "databricks-app/web/preview.html",
        "databricks-app/app.yaml",
        "resources/flowx_app/flowx_onboarding_app.yml",
        "databricks-bi/*.lvdash.json",
    ):
        assert rel in text, (
            f"branding/README.md no longer names {rel!r} in its hand-edited list, but a test "
            "asserts that file against branding.json."
        )
