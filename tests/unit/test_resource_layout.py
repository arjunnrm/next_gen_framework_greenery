"""Structural guard: ``resources/`` stays grouped, and every path inside a resource resolves.

``resources/`` used to be flat -- 95 YAML files in one directory, included by a single
``resources/*.yml`` glob. It is now grouped one folder per purpose (app, config jobs,
observability, BT-fixture tests, TC-* feature tests, stability tests, sample jobs), which buys a
readable tree
and a `--select`-able deploy, and costs two failure modes that no other test in this repo can see:

1. **A relative path left at ``../``.** DABs resolves a relative path against the file that
   declares it, so moving a resource one level down makes every ``../notebooks/...`` and
   ``../dist/*.whl`` inside it wrong. ``databricks bundle validate`` does **not** catch this --
   it validates config shape, not that a notebook exists at the path -- so a stale ``../`` gets
   through review and fails at deploy time, or uploads a glob that matched nothing.

2. **A group folder with no ``include:`` line.** ``databricks.yml`` lists each group explicitly
   (there is no recursive ``**`` glob). A new folder added without its include line produces a
   resource that is version-controlled, reviewed, and *never deployed* -- and, symmetrically, an
   include line whose folder is gone is a pattern matching nothing.

Both are cheap to assert from disk and impossible to notice by reading a diff, which is exactly
the shape of guard this file exists for. Deliberately file-based: nothing here imports the
framework or talks to a workspace.

See ``agent_skills/reference/common_pitfalls.md`` entry 34 and ``databricks.yml``'s own
RESOURCE LAYOUT header comment.
"""

import pathlib
import re

import pytest
import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
RESOURCES = REPO_ROOT / "resources"
BUNDLE_FILE = REPO_ROOT / "databricks.yml"

#: The groups ``resources/`` is partitioned into. Add a folder here *and* to ``databricks.yml``'s
#: ``include:`` list -- ``test_every_group_folder_is_included`` asserts the two agree.
EXPECTED_GROUPS = {
    "flowx_app",
    "flowx_bootstrap",
    "flowx_bi",
    "flowx_config_jobs",
    "observability",
    "bt_tests",
    "feature_tests",
    "stability_tests",
    "sample_jobs",
    "v0_0_2_tests",
    "uc3",
    "uc6",
    "uc7",
}

#: Groups whose ``include:`` line must exist even while the folder itself is still landing (or
#: sits empty). ``sample_jobs`` is being populated by a parallel workstream, so its folder may be
#: absent, empty, or full when this test runs -- all three must pass. An include glob matching
#: nothing is valid DABs config (``resources/stability_tests/*.yml`` matched nothing for weeks and
#: every ``bundle validate`` passed), so tolerating absence here weakens no other assertion.
#: uc3/uc7 are listed in EXPECTED_GROUPS so their folders are not flagged as unexpected, but
#: they are UNTRACKED in git (a parallel workstream's in-flight work), so a fresh worktree or
#: clone legitimately has neither. Their absence must not fail this test.
MAY_BE_ABSENT_GROUPS = {"sample_jobs", "uc3", "uc7"}

#: Groups whose folder exists and is deployed, but whose ``include:`` line is deliberately
#: commented out in databricks.yml -- the everyday deploy is scoped, and a test/reference
#: group is uncommented only when that suite is actually being run. uc3 and uc7 predate uc6
#: and were added to resources/ without being registered here at all, which is why
#: test_group_folders_are_exactly_the_expected_set has been failing offline; listing them
#: fixes that rather than papering over it.
MAY_BE_UNINCLUDED_GROUPS = {
    "observability", "bt_tests", "feature_tests", "stability_tests", "sample_jobs",
    "v0_0_2_tests", "uc3", "uc7",
}

#: Resources the everyday scoped deploy names (see ``databricks.yml``'s THE USUAL DEPLOY block
#: and docs/09 Step 8). Each entry is ``<type>.<resource key>`` as ``--select`` spells it,
#: mapped to the file that must declare it.
USUAL_DEPLOY_SELECTION = {
    "apps.flowx_onboarding_app": "flowx_app/flowx_onboarding_app.yml",
    "volumes.onboarding_specs_volume": "flowx_app/flowx_onboarding_specs_volume.yml",
    "jobs.onboarding_job": "flowx_config_jobs/onboarding_job.yml",
    "jobs.framework_config_onboarding_job": (
        "flowx_config_jobs/framework_config_onboarding_job.yml"
    ),
    "volumes.framework_wheels_volume": "flowx_config_jobs/framework_wheels_volume.yml",
}

#: A relative path in a resource YAML: ``../`` runs, then the first path segment. The leading
#: ``(?<![.\w])`` is what keeps prose out of the match -- several headers contain an ellipsis
#: (``/Workspace/.../test_specs/``, ``.../landing/2026-08-28/``), and ``...`` ends in ``../``.
RELATIVE_PATH = re.compile(r"(?<![.\w])((?:\.\./)+)([A-Za-z0-9_.-]+)")


def _resource_files():
    return sorted(RESOURCES.glob("*/*.yml"))


def test_no_resource_yaml_sits_directly_in_resources_root():
    """The flat layout is gone, not merely supplemented.

    A file left in the root would still be *loaded* only if some include pattern matched it --
    and none does any more, so it would be a resource that silently stops deploying.
    """
    stray = sorted(p.name for p in RESOURCES.glob("*.yml"))
    assert stray == [], (
        f"resources/ root must hold no YAML, found {stray}. Move each into one of "
        f"{sorted(EXPECTED_GROUPS)} and confirm databricks.yml includes that folder."
    )


def test_group_folders_are_exactly_the_expected_set():
    on_disk = {p.name for p in RESOURCES.iterdir() if p.is_dir() and not p.name.startswith(("_", "."))}
    unexpected = on_disk - EXPECTED_GROUPS
    assert not unexpected, (
        f"resources/ holds group folders not in EXPECTED_GROUPS: {sorted(unexpected)}. Add each "
        "here and to databricks.yml's include: list, or move its files into an existing group."
    )
    missing = EXPECTED_GROUPS - on_disk - MAY_BE_ABSENT_GROUPS
    assert not missing, (
        f"expected group folders are gone from resources/: {sorted(missing)}. Restore them or "
        "remove them from EXPECTED_GROUPS *and* databricks.yml's include: list together."
    )


def test_every_group_folder_is_included_by_databricks_yml():
    """``include:`` is an explicit list, so a new folder is invisible until it is added."""
    bundle = yaml.safe_load(BUNDLE_FILE.read_text(encoding="utf-8"))
    included = {
        pattern.split("/")[1]
        for pattern in bundle["include"]
        if pattern.startswith("resources/")
    }
    must_be_included = EXPECTED_GROUPS - MAY_BE_UNINCLUDED_GROUPS
    assert must_be_included <= included, (
        "databricks.yml `include:` and the resources/ tree disagree. Missing include lines "
        f"deploy nothing: {sorted(must_be_included - included)}."
    )
    assert included <= EXPECTED_GROUPS, (
        f"stale include lines match no folder: {sorted(included - EXPECTED_GROUPS)}."
    )


@pytest.mark.parametrize("resource_file", _resource_files(), ids=lambda p: p.name)
def test_relative_paths_resolve_from_the_grouped_location(resource_file):
    """Every ``../``-rooted path in a grouped resource must exist on disk.

    This is the assertion that a bare ``../notebooks/...`` fails: from
    ``resources/<group>/`` it resolves to ``resources/notebooks/...``, which does not exist. A
    glob (``../../dist/*.whl``) is checked by its parent directory, since the wheel itself is a
    build artefact that need not be present.
    """
    text = resource_file.read_text(encoding="utf-8")
    for match in RELATIVE_PATH.finditer(text):
        # Reconstruct the whole path token so a glob's parent can be checked.
        start = match.start()
        token = re.match(r"[^\s'\"),;]+", text[start:]).group(0).rstrip(".,;)'\"")
        candidate = (resource_file.parent / token).resolve()
        target = candidate.parent if "*" in candidate.name else candidate
        assert target.exists(), (
            f"{resource_file.relative_to(REPO_ROOT)} references {token!r}, which resolves to "
            f"{target} -- nothing there. A resource in resources/<group>/ is two levels below "
            f"the repo root, so paths are '../../', not '../'."
        )


@pytest.mark.parametrize("resource_file", _resource_files(), ids=lambda p: p.name)
def test_no_single_level_relative_path_to_a_repo_root_directory(resource_file):
    """Belt-and-braces for the case a stale ``../x`` happens to name something that exists.

    ``resources/`` contains only group folders today, so no ``../<dir>`` can be valid -- stating
    that directly gives a clearer failure than waiting for a path-existence miss.
    """
    text = resource_file.read_text(encoding="utf-8")
    offenders = [
        f"{dots}{first}"
        for dots, first in RELATIVE_PATH.findall(text)
        if dots == "../"
    ]
    assert offenders == [], (
        f"{resource_file.relative_to(REPO_ROOT)} still has single-level relative paths "
        f"{offenders}; from resources/<group>/ they must be '../../'."
    )


@pytest.mark.parametrize(
    ("selector", "relative_path"), sorted(USUAL_DEPLOY_SELECTION.items())
)
def test_usual_deploy_selection_names_real_resources(selector, relative_path):
    """The documented ``--select`` list must keep naming resources that exist.

    ``--select`` takes a resource *key*, not a filename, and a typo or a rename is not an error
    the CLI reports usefully -- it just deploys less than intended.
    """
    resource_type, resource_key = selector.split(".")
    path = RESOURCES / relative_path
    assert path.exists(), f"{relative_path} is gone; update databricks.yml's --select example"
    declared = yaml.safe_load(path.read_text(encoding="utf-8"))["resources"]
    assert resource_type in declared, f"{relative_path} declares no {resource_type}"
    assert resource_key in declared[resource_type], (
        f"{relative_path} does not declare {resource_type}.{resource_key}"
    )
    assert selector in BUNDLE_FILE.read_text(encoding="utf-8"), (
        f"databricks.yml's THE USUAL DEPLOY --select example no longer names {selector}"
    )


# ---------------------------------------------------------------------------------------------
# UC container hierarchy (resources/flowx_bootstrap/)
# ---------------------------------------------------------------------------------------------
#
# On 2026-09-02 a first deploy to a fresh workspace (`arjun_2`) failed with
#
#     Error: cannot create resources.volumes.framework_wheels_volume:
#            Schema 'flowx.config' does not exist
#
# because the bundle declared its Volumes but not the schemas holding them. `bundle validate`
# cannot catch this -- the config shape is valid; only a deploy against a workspace missing the
# container discovers it. So the invariant is asserted from disk: **every schema_name a declared
# volume names must itself be a declared schema.**
#
# The catalog is deliberately NOT part of that chain -- it is a prerequisite created outside the
# bundle, and a `catalogs.*` resource cannot succeed on these Default-Storage workspaces. See
# `test_no_catalog_is_declared_as_a_bundle_resource` and
# `resources/flowx_bootstrap/README.md`.

#: The variables these resources interpolate, resolved to the value every current target sets.
#: ``catalog`` and ``schema`` are declared without a default in databricks.yml (each target must
#: set them), so the values are pinned here rather than read from the bundle's ``variables:``.
_VAR_VALUES = {"${var.catalog}": "flowx", "${var.schema}": "dev", "${var.spec_schema}": "config"}


def _resolve(value):
    """Substitute the ``${var.x}`` forms these bootstrap resources use, leaving others intact."""
    for token, resolved in _VAR_VALUES.items():
        value = value.replace(token, resolved)
    return value


def _declared(resource_type):
    """Map every declared resource of ``resource_type`` to its ``(key, body, file)``."""
    out = {}
    for path in _resource_files():
        loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for key, body in (loaded.get("resources") or {}).get(resource_type, {}).items():
            out[key] = (body, path)
    return out


def test_every_declared_volume_sits_in_a_declared_schema():
    """A Volume whose schema nothing declares fails the deploy, not the validate."""
    schemas = {
        f"{_resolve(body['catalog_name'])}.{_resolve(body['name'])}"
        for body, _ in _declared("schemas").values()
    }
    missing = {}
    for key, (body, path) in _declared("volumes").items():
        parent = f"{_resolve(body['catalog_name'])}.{_resolve(body['schema_name'])}"
        if parent not in schemas:
            missing[f"volumes.{key} ({path.relative_to(REPO_ROOT)})"] = parent
    assert not missing, (
        "these volumes name a schema no resource declares, so a first deploy to a fresh "
        f"workspace fails with \"Schema '<x>' does not exist\": {missing}. Declare each schema "
        "in resources/flowx_bootstrap/flowx_schemas.yml."
    )


def test_no_catalog_is_declared_as_a_bundle_resource():
    """``${var.catalog}`` is a PREREQUISITE, and a ``catalogs.*`` resource can only ever fail.

    A catalog resource was added on 2026-09-02 as the apparent completion of the container
    hierarchy, and removed the same day. Two independent blockers, verified live:

    * These accounts use UC **Default Storage**, so ``CREATE CATALOG`` with no ``MANAGED
      LOCATION`` is rejected outright -- ``Metastore storage root URL does not exist ...
      (400 INVALID_STATE)``. Supplying one is not a fix either: a Default-Storage catalog's
      ``storage_root`` is an account-managed bucket path containing metastore and catalog UUIDs
      generated at create time, so it cannot be committed to YAML and differs per workspace.
    * ``flowx`` already exists on all three targets, each created outside this bundle.

    And the failure is not contained: DABs propagates it down the dependency edge it creates, so
    the failing catalog took the schemas with it (``cannot create resources.schemas.config_schema:
    dependency failed: resources.catalogs.flowx_catalog``) -- strictly worse than declaring no
    catalog at all. See ``resources/flowx_bootstrap/README.md``.
    """
    declared = _declared("catalogs")
    assert declared == {}, (
        f"a catalogs.* resource is declared again: {sorted(declared)}. It cannot succeed on these "
        "workspaces -- UC Default Storage rejects CREATE CATALOG without a MANAGED LOCATION, the "
        "storage_root is an account-managed path that cannot live in YAML, and the catalog already "
        "exists on every target. Worse, its failure propagates down the dependency edge and takes "
        "schemas.config_schema with it. Read resources/flowx_bootstrap/README.md before "
        "re-adding it; create the catalog in the UI instead."
    )


def test_declared_schemas_name_the_catalog_variable():
    """With no catalog resource, every schema must still point at ``${var.catalog}``.

    A hard-coded catalog name here would deploy the framework's schemas into the wrong catalog on
    any target that overrides ``catalog`` -- silently, since the create would succeed.
    """
    offenders = {
        f"schemas.{key} ({path.relative_to(REPO_ROOT)})": body["catalog_name"]
        for key, (body, path) in _declared("schemas").items()
        if body["catalog_name"] != "${var.catalog}"
    }
    assert not offenders, (
        f"these schemas hard-code a catalog instead of using ${{var.catalog}}: {offenders}"
    )


def test_every_declared_schema_is_protected_from_destroy():
    """``prevent_destroy`` is what stops an editing mistake becoming data loss.

    DABs deletes any resource missing from the config (see databricks.yml's RESOURCE LAYOUT
    header), and these schemas hold the control tables, the published wheels, the authored
    onboarding specs and the sample datasets -- so removing a file must not be able to drop one.
    """
    unguarded = [
        f"schemas.{key} ({path.relative_to(REPO_ROOT)})"
        for key, (body, path) in _declared("schemas").items()
        if (body.get("lifecycle") or {}).get("prevent_destroy") is not True
    ]
    assert not unguarded, (
        f"these schemas do not set lifecycle.prevent_destroy: true: {unguarded}. Without it, "
        "removing the file (or a bundle destroy) asks UC to drop the schema and everything in it."
    )


def test_sample_suite_volumes_match_the_provisioning_notebook():
    """The declared Volumes and the seed notebook's ``VOLUMES`` tuple must not drift.

    The notebook keeps its ``CREATE VOLUME IF NOT EXISTS`` calls so it stays runnable standalone.
    If a volume is added there but not here, a deploy alone stops being enough to make the sample
    suite runnable -- which is the whole property resources/flowx_bootstrap/ adds.
    """
    notebook = (
        REPO_ROOT
        / "notebooks"
        / "00_seed_sample_data"
        / "04_seed_sample_00_provision_sample_schema.py"
    )
    if not notebook.exists():  # pragma: no cover - the notebook is expected to be present
        pytest.skip(f"{notebook.name} is absent; nothing to compare against")
    match = re.search(r"^VOLUMES\s*=\s*\(([^)]*)\)", notebook.read_text(encoding="utf-8"), re.M)
    assert match, f"could not find a VOLUMES tuple in {notebook.name}"
    in_notebook = set(re.findall(r'"([^"]+)"', match.group(1)))
    declared = {
        _resolve(body["name"])
        for body, _ in _declared("volumes").values()
        if _resolve(body["schema_name"]) == "flowx_sample"
    }
    assert declared == in_notebook, (
        "resources/flowx_bootstrap/flowx_sample_volumes.yml and "
        f"{notebook.name}'s VOLUMES tuple disagree. Only the notebook has: "
        f"{sorted(in_notebook - declared)}; only the bundle has: {sorted(declared - in_notebook)}."
    )


# --- The new-workspace `wheels_root` bootstrap (v1.7.4) ---------------------------------------
#
# `workspace.artifact_path` points into a UC Volume that this same bundle declares, and DABs
# refuses such a path until the Volume exists -- the check runs during config resolution, before
# any resource is created, so the deploy that would create it is the one being rejected. The
# escape hatch is `${var.wheels_root}`: one `--var` override moves the wheel out of the Volume
# for a single bootstrap deploy. That only works while EVERY target composes its artifact_path
# from the variable; a target that hardcodes the literal path silently opts out of the fix and
# resurrects the hand-edit that v1.7.4 removed. These two tests keep that from happening.


def test_every_target_composes_artifact_path_from_wheels_root():
    bundle = yaml.safe_load(BUNDLE_FILE.read_text(encoding="utf-8"))
    offenders = {
        name: target["workspace"]["artifact_path"]
        for name, target in (bundle.get("targets") or {}).items()
        if "artifact_path" in (target.get("workspace") or {})
        and "${var.wheels_root}" not in target["workspace"]["artifact_path"]
    }
    assert not offenders, (
        "these targets hardcode artifact_path instead of composing it from ${var.wheels_root}: "
        f"{offenders}. That opts them out of the one-command new-workspace bootstrap "
        "(docs/onboarding/05_new_workspace_bootstrap.md) and forces the old hand-edit of "
        "databricks.yml. Use `artifact_path: ${var.wheels_root}/${var.framework_version}`."
    )


def test_wheels_root_defaults_into_the_declared_wheels_volume():
    """The default must resolve to the Volume the bundle declares, not to a workspace path.

    An override is a *bootstrap-only* argument. If the committed default ever pointed outside
    the Volume, every ordinary deploy would quietly publish the wheel somewhere unmanaged and
    the v1.6.0 shared-artifact-path guarantee would be gone with no error anywhere.
    """
    bundle = yaml.safe_load(BUNDLE_FILE.read_text(encoding="utf-8"))
    default = (bundle["variables"]["wheels_root"] or {}).get("default")
    assert default == "/Volumes/${var.catalog}/config/wheels", (
        f"wheels_root's committed default is {default!r}. It must be the declared wheels Volume "
        "(/Volumes/${var.catalog}/config/wheels) -- an override belongs on the command line for "
        "the one-time bootstrap, never in the file."
    )
