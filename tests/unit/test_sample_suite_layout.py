"""Structural guard for the ``metaflow_sample`` reference suite's job wiring.

Three properties of the suite are asserted here, none of which any other test can see and none
of which ``databricks bundle validate`` checks (it validates config *shape*, not the shape of
this suite's conventions):

1. **Seeding lives in exactly one job.** Every sample job used to inline its own three
   ``seed_iteration<N>`` notebook tasks. They are now consolidated into the single
   ``metaflow_sample_seed_job``, one chain per sample. The regression this guards against is a
   seed notebook creeping back into a sample job -- which would silently re-land fixtures a
   developer expected the seed job to own, and re-introduce the six-way concurrent
   ``CREATE ... IF NOT EXISTS`` race the seed job's serial root task exists to prevent.

2. **Every sample's spec is published into the one reference Volume.** Each sample job's last
   task must be a ``store_sample_config`` naming *its own* spec, and
   ``09a_store_sample_config.py`` must target exactly one Volume. A sample whose spec never
   reaches ``/Volumes/<catalog>/metaflow_sample/sample_configs/`` leaves a developer browsing
   the schema with tables and no document explaining them.

3. **The suite is internally complete.** Every sample job has a matching pipeline resource and
   a matching spec file, and every spec file has a job -- the three lists cannot drift apart
   without one of them naming something that does not exist.

Deliberately file-based: nothing here imports the framework or talks to a workspace. See
``tests/unit/test_resource_layout.py`` for the complementary path-resolution guard that applies
to *every* resource, and ``tests/unit/test_sample_specs.py`` for the specs' own content.
"""

import pathlib
import re

import pytest
import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SAMPLE_JOBS_DIR = REPO_ROOT / "resources" / "sample_jobs"
SEED_NOTEBOOK_DIR = REPO_ROOT / "notebooks" / "00_seed_sample_data"
SAMPLES_SPEC_DIR = REPO_ROOT / "resources" / "sample_jobs" / "onboarding"
STORE_CONFIG_NOTEBOOK = REPO_ROOT / "notebooks" / "09_sample_reference" / "09a_store_sample_config.py"

#: The one job that owns every fixture the suite consumes.
SEED_JOB_FILE = "metaflow_sample_seed_job.yml"
SEED_JOB_KEY = "metaflow_sample_seed_job"

#: The suite's samples, by the two-digit id that prefixes their job, pipeline, spec and seed
#: notebook. Adding a sample means adding it here *and* shipping all four artefacts.
SAMPLE_IDS = ("01", "02", "03", "04", "05", "06")

#: Each sample's seed notebook, relative to ``notebooks/00_seed_sample_data/``.
SEED_NOTEBOOKS = {
    "01": "04_seed_sample_01_multi_scd_data.py",
    "02": "04_seed_sample_02_zip_ingestion_data.py",
    "03": "04_seed_sample_03_multi_table_recon_data.py",
    "04": "04_seed_sample_04_export_encrypt_zip_data.py",
    "05": "04_seed_sample_05_encrypted_ingestion_data.py",
    "06": "04_seed_sample_06_asn1_tap3_data.py",
}

#: The seed job's serial root -- see its own YAML header for why it cannot be dropped.
PROVISION_TASK_KEY = "provision_sample_schema"

#: The rest of the seed job's provisioning chain, moved out of the six sample jobs so each of
#: those is exactly two tasks.
SETUP_TASK_KEY = "setup_control_tables"
ONBOARD_TASK_KEY = "onboard_all_samples"

#: The two -- and only two -- tasks a sample job may declare.
PIPELINE_TASK_KEY = "pipeline_task"
OBSERVABILITY_TASK_KEY = "observability_task"
PROVISION_NOTEBOOK = "04_seed_sample_00_provision_sample_schema.py"

ITERATIONS = ("1", "2", "3")

#: The single Volume every sample spec is published into.
SAMPLE_CONFIGS_VOLUME = "sample_configs"

#: A numbered sample job file: metaflow_sample_<id>_<name>_job.yml. Excludes the seed job, whose
#: name matches ``metaflow_sample_*_job.yml`` too.
SAMPLE_JOB_FILE = re.compile(r"^metaflow_sample_(\d{2})_.+_job\.yml$")
SAMPLE_PIPELINE_FILE = re.compile(r"^metaflow_sample_(\d{2})_.+_pipeline\.yml$")


def _load(path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _sample_job_files():
    return sorted(p for p in SAMPLE_JOBS_DIR.glob("*.yml") if SAMPLE_JOB_FILE.match(p.name))


def _sample_pipeline_files():
    return sorted(p for p in SAMPLE_JOBS_DIR.glob("*.yml") if SAMPLE_PIPELINE_FILE.match(p.name))


def _tasks(job_yaml):
    """The single job's task list, whatever its resource key."""
    jobs = job_yaml["resources"]["jobs"]
    assert len(jobs) == 1, f"expected exactly one job per file, found {sorted(jobs)}"
    return next(iter(jobs.values()))["tasks"]


def _notebook_path(task):
    return (task.get("notebook_task") or {}).get("notebook_path", "")


# ----------------------------------------------------------------- 1. seeding is centralised


def test_the_common_seed_job_exists():
    path = SAMPLE_JOBS_DIR / SEED_JOB_FILE
    assert path.exists(), f"{SEED_JOB_FILE} is the suite's only seed owner and is missing"
    assert SEED_JOB_KEY in _load(path)["resources"]["jobs"], (
        f"{SEED_JOB_FILE} must declare jobs.{SEED_JOB_KEY} -- `bundle run` and every doc name it"
    )


def test_seed_job_provisions_storage_before_anything_else():
    """The serial root exists and every chain's first task waits on it.

    Unity Catalog's ``CREATE ... IF NOT EXISTS`` is idempotent in intent but not atomic; six
    seed chains starting at once is a real create race (common_pitfalls.md entry 7).
    """
    tasks = {t["task_key"]: t for t in _tasks(_load(SAMPLE_JOBS_DIR / SEED_JOB_FILE))}
    assert PROVISION_TASK_KEY in tasks, (
        f"{SEED_JOB_FILE} must open with a serial '{PROVISION_TASK_KEY}' task"
    )
    assert PROVISION_NOTEBOOK in _notebook_path(tasks[PROVISION_TASK_KEY])
    assert not tasks[PROVISION_TASK_KEY].get("depends_on"), (
        f"'{PROVISION_TASK_KEY}' is the root task and must depend on nothing"
    )

    # The provisioning chain is serial: schema -> control tables -> onboarding. Every seed
    # chain then waits on the LAST link, so no fixture lands before its group is onboarded.
    assert [d["task_key"] for d in tasks[SETUP_TASK_KEY].get("depends_on", [])] == [
        PROVISION_TASK_KEY
    ], f"'{SETUP_TASK_KEY}' must depend on '{PROVISION_TASK_KEY}'"
    assert [d["task_key"] for d in tasks[ONBOARD_TASK_KEY].get("depends_on", [])] == [
        SETUP_TASK_KEY
    ], f"'{ONBOARD_TASK_KEY}' must depend on '{SETUP_TASK_KEY}' -- control tables first"

    for sample_id in SAMPLE_IDS:
        first = tasks[f"seed_sample_{sample_id}_iteration1"]
        depends = [d["task_key"] for d in first.get("depends_on", [])]
        assert depends == [ONBOARD_TASK_KEY], (
            f"seed_sample_{sample_id}_iteration1 must depend only on '{ONBOARD_TASK_KEY}', "
            f"got {depends}"
        )


def test_seed_job_owns_all_provisioning_removed_from_sample_jobs():
    """Control-table setup and spec onboarding moved OUT of the six sample jobs and INTO the
    one seed job, so a cloned sample job is exactly pipeline_task + observability_task.

    Guards the removal: if onboarding silently vanished from BOTH places, every sample would
    fail at runtime with `FrameworkConfigError: No active dataflow_group_spec row found`."""
    tasks = {t["task_key"]: t for t in _tasks(_load(SAMPLE_JOBS_DIR / SEED_JOB_FILE))}

    assert SETUP_TASK_KEY in tasks, (
        f"{SEED_JOB_FILE} must own '{SETUP_TASK_KEY}' now that sample jobs do not"
    )
    assert ONBOARD_TASK_KEY in tasks, (
        f"{SEED_JOB_FILE} must own '{ONBOARD_TASK_KEY}' now that sample jobs do not -- "
        "without it no sample pipeline can resolve its dataflow_group_spec row"
    )

    onboard = tasks[ONBOARD_TASK_KEY]
    assert "run_job_task" in onboard, (
        f"'{ONBOARD_TASK_KEY}' must delegate via run_job_task, never an inline "
        "02_onboarding_engine.py notebook_task (AGENTS.md)"
    )
    spec_dir = onboard["run_job_task"]["job_parameters"]["spec_dir"]
    assert spec_dir.endswith("resources/sample_jobs/onboarding"), (
        f"'{ONBOARD_TASK_KEY}' must onboard the bundle's spec directory, got {spec_dir!r}"
    )


@pytest.mark.parametrize("sample_id", SAMPLE_IDS)
def test_seed_job_runs_every_sample_iteration_in_order(sample_id):
    """One chain per sample, three iterations, strictly ordered.

    Several seeds are cumulative by construction (Sample 01's snapshot feed is additive,
    Sample 03 layers drift on the previous slice), so a parallel or reordered chain would land
    fixtures that do not match what the specs describe.
    """
    tasks = {t["task_key"]: t for t in _tasks(_load(SAMPLE_JOBS_DIR / SEED_JOB_FILE))}
    for iteration in ITERATIONS:
        key = f"seed_sample_{sample_id}_iteration{iteration}"
        assert key in tasks, f"{SEED_JOB_FILE} is missing task '{key}'"
        task = tasks[key]
        assert SEED_NOTEBOOKS[sample_id] in _notebook_path(task), (
            f"{key} must run {SEED_NOTEBOOKS[sample_id]}, got {_notebook_path(task)!r}"
        )
        assert task["notebook_task"]["base_parameters"]["iteration"] == iteration, (
            f"{key} must pass iteration='{iteration}'"
        )
        if iteration != "1":
            previous = f"seed_sample_{sample_id}_iteration{int(iteration) - 1}"
            depends = [d["task_key"] for d in task.get("depends_on", [])]
            assert depends == [previous], (
                f"{key} must depend only on {previous} (iterations are cumulative), got {depends}"
            )


@pytest.mark.parametrize("job_file", _sample_job_files(), ids=lambda p: p.name)
def test_no_sample_job_inlines_a_seed_notebook(job_file):
    """THE regression guard: seeding belongs to metaflow_sample_seed_job and nowhere else."""
    offenders = [
        task["task_key"]
        for task in _tasks(_load(job_file))
        if "00_seed_sample_data" in _notebook_path(task)
    ]
    assert offenders == [], (
        f"{job_file.name} inlines seed notebook task(s) {offenders}. Seeding moved to "
        f"{SEED_JOB_FILE} -- add the iteration there instead, or the fixtures land twice."
    )


def test_every_seed_notebook_referenced_by_the_seed_job_exists():
    for notebook in [PROVISION_NOTEBOOK, *SEED_NOTEBOOKS.values()]:
        assert (SEED_NOTEBOOK_DIR / notebook).exists(), (
            f"{SEED_NOTEBOOK_DIR.name}/{notebook} is referenced by {SEED_JOB_FILE} but absent"
        )


# ------------------------------------------- 2. every spec reaches the one reference Volume


def test_store_sample_config_targets_exactly_one_volume():
    """The publishing notebook must name a single Volume -- ``sample_configs`` -- so 'the sample
    job JSONs live in one Volume in the sample schema' stays true by construction."""
    source = STORE_CONFIG_NOTEBOOK.read_text(encoding="utf-8")
    created = set(re.findall(r"CREATE VOLUME IF NOT EXISTS \{CATALOG\}\.\{SAMPLE_SCHEMA\}\.(\w+)", source))
    assert created == {SAMPLE_CONFIGS_VOLUME}, (
        f"09a_store_sample_config.py must provision exactly the one "
        f"'{SAMPLE_CONFIGS_VOLUME}' Volume; found {sorted(created) or 'none'}"
    )
    assert f'TARGET_DIR = f"/Volumes/{{CATALOG}}/{{SAMPLE_SCHEMA}}/{SAMPLE_CONFIGS_VOLUME}"' in source, (
        f"09a_store_sample_config.py must publish into /Volumes/<catalog>/<sample schema>/"
        f"{SAMPLE_CONFIGS_VOLUME}"
    )


@pytest.mark.parametrize("job_file", _sample_job_files(), ids=lambda p: p.name)
def test_every_sample_job_is_exactly_two_tasks(job_file):
    """The developer blueprint: pipeline_task + observability_task, nothing else.

    A sample job is a pattern developers clone. Every extra task is one they must understand
    and then delete. Provisioning belongs to the seed job (see
    ``test_seed_job_owns_all_provisioning_removed_from_sample_jobs``); this asserts none of it
    creeps back in."""
    keys = [t["task_key"] for t in _tasks(_load(job_file))]
    assert keys == [PIPELINE_TASK_KEY, OBSERVABILITY_TASK_KEY], (
        f"{job_file.name} must declare exactly "
        f"['{PIPELINE_TASK_KEY}', '{OBSERVABILITY_TASK_KEY}'], found {keys}"
    )


@pytest.mark.parametrize("job_file", _sample_job_files(), ids=lambda p: p.name)
def test_sample_job_observability_follows_its_own_pipeline(job_file):
    """The telemetry task must export the update THIS job just ran -- a stale or missing
    pipeline_task_run_id silently exports the wrong (or no) update."""
    tasks = {t["task_key"]: t for t in _tasks(_load(job_file))}
    obs = tasks[OBSERVABILITY_TASK_KEY]
    assert [d["task_key"] for d in obs.get("depends_on", [])] == [PIPELINE_TASK_KEY], (
        f"{job_file.name}'s '{OBSERVABILITY_TASK_KEY}' must depend on '{PIPELINE_TASK_KEY}'"
    )
    params = obs["notebook_task"]["base_parameters"]
    assert params["pipeline_task_run_id"] == "{{tasks.%s.run_id}}" % PIPELINE_TASK_KEY, (
        f"{job_file.name} must pass the run_id of its own '{PIPELINE_TASK_KEY}' task"
    )
    sample_id = SAMPLE_JOB_FILE.match(job_file.name).group(1)
    assert f"sample_{sample_id}" in params["dataflow_group_id"], (
        f"{job_file.name} exports telemetry for {params['dataflow_group_id']!r}, "
        f"which is not sample {sample_id}'s own group"
    )


# ------------------------------------------------------------------ 3. the suite is complete


def test_jobs_pipelines_and_specs_cover_the_same_samples():
    job_ids = {SAMPLE_JOB_FILE.match(p.name).group(1) for p in _sample_job_files()}
    pipeline_ids = {SAMPLE_PIPELINE_FILE.match(p.name).group(1) for p in _sample_pipeline_files()}
    spec_ids = {p.name[len("sample_") : len("sample_") + 2] for p in SAMPLES_SPEC_DIR.glob("sample_*.json")}

    assert job_ids == set(SAMPLE_IDS), f"sample job files cover {sorted(job_ids)}, expected {list(SAMPLE_IDS)}"
    assert pipeline_ids == set(SAMPLE_IDS), (
        f"sample pipeline files cover {sorted(pipeline_ids)}, expected {list(SAMPLE_IDS)}"
    )
    assert spec_ids == set(SAMPLE_IDS), f"sample specs cover {sorted(spec_ids)}, expected {list(SAMPLE_IDS)}"


@pytest.mark.parametrize("job_file", _sample_job_files(), ids=lambda p: p.name)
def test_every_sample_job_runs_exactly_one_pipeline_update(job_file):
    """With all three iterations seeded up front, one update ingests them all -- the three
    ``run_pipeline_iteration<N>`` tasks the suite used to carry would be no-ops 2 and 3."""
    pipeline_tasks = [t["task_key"] for t in _tasks(_load(job_file)) if "pipeline_task" in t]
    assert pipeline_tasks == [PIPELINE_TASK_KEY], (
        f"{job_file.name} must run its pipeline exactly once, in a task called "
        f"'{PIPELINE_TASK_KEY}'; "
        f"found {pipeline_tasks}"
    )


def test_no_sample_job_onboards_or_seeds_anything():
    """Onboarding and seeding are the seed job's, not the blueprint's.

    Replaces the old per-job `test_every_sample_job_onboards_through_the_generic_onboarding_job`:
    a sample job must now carry NO onboarding task at all, inline or delegated."""
    for job_file in _sample_job_files():
        tasks = _tasks(_load(job_file))
        inline = [t["task_key"] for t in tasks if "02_onboarding_engine" in _notebook_path(t)]
        assert inline == [], (
            f"{job_file.name} inlines the onboarding engine in {inline}; onboarding belongs to "
            f"{SEED_JOB_FILE}'s '{ONBOARD_TASK_KEY}' task"
        )
        delegated = [t["task_key"] for t in tasks if "run_job_task" in t]
        assert delegated == [], (
            f"{job_file.name} still delegates onboarding in {delegated}; it moved to "
            f"{SEED_JOB_FILE}'s '{ONBOARD_TASK_KEY}' task"
        )
