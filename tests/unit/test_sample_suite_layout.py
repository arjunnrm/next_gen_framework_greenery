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
SAMPLES_SPEC_DIR = REPO_ROOT / "metaflow_testing" / "samples"
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

    for sample_id in SAMPLE_IDS:
        first = tasks[f"seed_sample_{sample_id}_iteration1"]
        depends = [d["task_key"] for d in first.get("depends_on", [])]
        assert depends == [PROVISION_TASK_KEY], (
            f"seed_sample_{sample_id}_iteration1 must depend only on '{PROVISION_TASK_KEY}', "
            f"got {depends}"
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
def test_every_sample_job_publishes_its_own_spec(job_file):
    tasks = _tasks(_load(job_file))
    store = [t for t in tasks if t["task_key"] == "store_sample_config"]
    assert len(store) == 1, (
        f"{job_file.name} must end with exactly one 'store_sample_config' task, found {len(store)}"
    )
    sample_id = SAMPLE_JOB_FILE.match(job_file.name).group(1)
    spec_paths = store[0]["notebook_task"]["base_parameters"]["spec_paths"]
    assert f"/samples/sample_{sample_id}_" in spec_paths, (
        f"{job_file.name}'s store_sample_config publishes {spec_paths!r}, which is not "
        f"sample {sample_id}'s own spec"
    )
    referenced = spec_paths.rsplit("/", 1)[-1]
    assert (SAMPLES_SPEC_DIR / referenced).exists(), (
        f"{job_file.name} publishes '{referenced}', which does not exist in metaflow_testing/samples/"
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
    assert pipeline_tasks == ["run_pipeline"], (
        f"{job_file.name} must run its pipeline exactly once, in a task called 'run_pipeline'; "
        f"found {pipeline_tasks}"
    )


@pytest.mark.parametrize("job_file", _sample_job_files(), ids=lambda p: p.name)
def test_every_sample_job_onboards_through_the_generic_onboarding_job(job_file):
    """Onboarding is delegated via run_job_task -- never an inline 02_onboarding_engine.py."""
    tasks = _tasks(_load(job_file))
    inline = [t["task_key"] for t in tasks if "02_onboarding_engine" in _notebook_path(t)]
    assert inline == [], (
        f"{job_file.name} inlines the onboarding engine in {inline}; delegate to "
        "${resources.jobs.onboarding_job.id} via run_job_task instead"
    )
    delegated = [t for t in tasks if "run_job_task" in t]
    assert len(delegated) == 1, (
        f"{job_file.name} must delegate onboarding through exactly one run_job_task, "
        f"found {len(delegated)}"
    )
