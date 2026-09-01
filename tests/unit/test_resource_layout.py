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
    "metaflow_app",
    "metaflow_config_jobs",
    "observability",
    "bt_tests",
    "feature_tests",
    "stability_tests",
    "sample_jobs",
}

#: Groups whose ``include:`` line must exist even while the folder itself is still landing (or
#: sits empty). ``sample_jobs`` is being populated by a parallel workstream, so its folder may be
#: absent, empty, or full when this test runs -- all three must pass. An include glob matching
#: nothing is valid DABs config (``resources/stability_tests/*.yml`` matched nothing for weeks and
#: every ``bundle validate`` passed), so tolerating absence here weakens no other assertion.
MAY_BE_ABSENT_GROUPS = {"sample_jobs"}

#: Resources the everyday scoped deploy names (see ``databricks.yml``'s THE USUAL DEPLOY block
#: and docs/09 Step 8). Each entry is ``<type>.<resource key>`` as ``--select`` spells it,
#: mapped to the file that must declare it.
USUAL_DEPLOY_SELECTION = {
    "apps.metaflow_onboarding_app": "metaflow_app/metaflow_onboarding_app.yml",
    "volumes.onboarding_specs_volume": "metaflow_app/metaflow_onboarding_specs_volume.yml",
    "jobs.onboarding_job": "metaflow_config_jobs/onboarding_job.yml",
    "jobs.framework_config_onboarding_job": (
        "metaflow_config_jobs/framework_config_onboarding_job.yml"
    ),
    "volumes.framework_wheels_volume": "metaflow_config_jobs/framework_wheels_volume.yml",
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
    assert included == EXPECTED_GROUPS, (
        "databricks.yml `include:` and the resources/ tree disagree. Missing include lines "
        f"deploy nothing: {sorted(EXPECTED_GROUPS - included)}; stale include lines match "
        f"nothing: {sorted(included - EXPECTED_GROUPS)}."
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
