"""
Application settings loader and schema for the Metaflow Onboarding App.
Loads config/index.json and validates all environment/integration properties.

Brand strings and the environment-variable prefix come from
``server/branding_generated.py``, which ``scripts/apply_branding.py`` emits from
``branding/branding.json``. The repo-root ``branding/`` package is NOT uploaded to
Databricks Apps (the app resource sets ``source_code_path: "../../databricks-app"``),
so the generated in-app copy is the only thing that exists at runtime.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field

from server.branding_generated import APP_TITLE, DOCS_EXTERNAL_BASE_URL, env_var


class AppInfo(BaseModel):
    title: str = APP_TITLE
    framework_version: str = "1.5.0"
    environment_label: str = "dev"
    support_contact: str = "data-platform@example.com"


class AuthConfig(BaseModel):
    mode: str = "obo"
    fallback_to_sp: bool = False


class WorkspaceConfig(BaseModel):
    host: str = "https://dbc-2f6b7d4f-8c5b.cloud.databricks.com"
    run_url_template: str = "{host}/#job/{job_id}/run/{run_id}"
    pipeline_url_template: str = "{host}/#joblist/pipelines/{pipeline_id}"


class TemplateVar(BaseModel):
    label: str
    default: str
    required: bool = True


class StorageRoot(BaseModel):
    id: str
    label: str
    kind: str  # "volume" | "workspace"
    path: str
    read: bool = True
    write: bool = True


class SpecStorage(BaseModel):
    roots: List[StorageRoot]
    default_root: str = "vol_specs"
    allowed_extensions: List[str] = [".json", ".yaml", ".yml"]
    filename_template: str = "{dataflow_group_id}_{timestamp}.json"


class ActionConfig(BaseModel):
    enabled: bool = True
    label: str
    mode: str = "job"  # "job" | "pipeline" | "local"
    job_id: Optional[int] = None
    pipeline_id: Optional[str] = None
    confirm: bool = False
    confirm_text: Optional[str] = None
    requires: List[str] = []
    parameter_map: Dict[str, str] = {}
    stages: List[str] = []
    poll_interval_ms: int = 2000
    timeout_ms: int = 600000


class DocsConfig(BaseModel):
    mode: str = "embedded"  # "embedded" | "external" | "proxy"
    base_url: str = "/docs/"
    external_base_url: str = DOCS_EXTERNAL_BASE_URL
    attribute_reference_page: str = "00_master_reference_index/"
    open_in: str = "panel"


class FeaturesConfig(BaseModel):
    load_existing_spec: bool = True
    export_download: bool = True
    export_to_workspace: bool = True
    control_table_browse: bool = False
    yaml_output: bool = True
    diff_before_onboard: bool = True


class LimitsConfig(BaseModel):
    max_flows_per_kind: int = 50
    max_spec_bytes: int = 2097152


class AppSettings(BaseModel):
    schema_version: str = "1.0"
    config_dir: Path = Field(default_factory=lambda: Path(__file__).parent.parent / "config")
    app: AppInfo = Field(default_factory=AppInfo)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    workspace: WorkspaceConfig = Field(default_factory=WorkspaceConfig)
    template_variables: Dict[str, TemplateVar] = Field(default_factory=dict)
    spec_storage: SpecStorage
    actions: Dict[str, ActionConfig] = Field(default_factory=dict)
    docs: DocsConfig = Field(default_factory=DocsConfig)
    features: FeaturesConfig = Field(default_factory=FeaturesConfig)
    limits: LimitsConfig = Field(default_factory=LimitsConfig)


# Environment overrides, applied on top of config/index.json.
#
# config/index.json is checked in, so every value in it is a value for *one* workspace.
# Deploying this app to a second workspace previously meant hand-editing the workspace
# host, the onboarding job id and the storage roots in a tracked file — and a stale
# host or a job id from the wrong workspace fails at run time, not at deploy time.
#
# These variables let the bundle supply those four things at deploy time
# (resources/flowx_app/flowx_onboarding_app.yml sets them from ${workspace.host},
# ${resources.jobs.onboarding_job.id} and the target's own catalog/schema variables), so
# the same source tree deploys unchanged to any workspace. Anything not set falls back to
# config/index.json exactly as before.
#
# The names are DERIVED from the brand, not pasted: env_var() prefixes each with
# branding.json's env.prefix ("METAFLOW"), so a future customer rebrand moves every
# variable by editing one config key instead of a dozen literals.
#
# There is deliberately NO fallback to the legacy FLOWX_* names. The bundle also
# writes the unbranded aliases ONBOARDING_JOB_ID / DATABRICKS_ONBOARDING_JOB_ID and
# this module falls back to DATABRICKS_HOST, so a half-finished rename ALREADY has
# three ways to look like it worked. Adding a legacy-name fallback on top would mean
# an app deployed with only FLOWX_* keeps serving happily while every operator, doc
# and dashboard says METAFLOW_* — the silent-misconfiguration failure this rename
# exists to eliminate. A missing METAFLOW_* variable must degrade the same way a
# missing variable always has: to the checked-in config/index.json default, visibly.
ENV_WORKSPACE_HOST = env_var("WORKSPACE_HOST")
ENV_ONBOARDING_JOB_ID = env_var("ONBOARDING_JOB_ID")
ENV_VALIDATE_JOB_ID = env_var("VALIDATE_JOB_ID")
ENV_SPEC_CATALOG = env_var("SPEC_CATALOG")
ENV_SPEC_ENV = env_var("SPEC_ENV")
ENV_SPEC_VOLUME_ROOT = env_var("SPEC_VOLUME_ROOT")
ENV_SPEC_WORKSPACE_ROOT = env_var("SPEC_WORKSPACE_ROOT")

# Test-only hook. Read here and in deps.py / clients/dbx.py; set by the test suite.
ENV_FAKE_DBX = env_var("FAKE_DBX")

# The config-file pointer. Read in load_settings(); written by databricks-app/app.yaml
# (local runs) and by the bundle app resource (deploy, which supersedes app.yaml).
ENV_APP_CONFIG = env_var("APP_CONFIG")

# Log level. Read by logging_setup.setup_logging(); same two writers as ENV_APP_CONFIG.
ENV_LOG_LEVEL = env_var("LOG_LEVEL")


def _env(name: str) -> Optional[str]:
    value = os.environ.get(name, "").strip()
    return value or None


def apply_env_overrides(settings: AppSettings) -> AppSettings:
    """Overlay deploy-time environment variables onto the loaded configuration."""
    # Workspace host. DATABRICKS_HOST is injected into every Databricks App by the
    # platform, so it is a correct last resort that needs no bundle wiring at all —
    # far better than serving a checked-in host belonging to a different workspace.
    host = _env(ENV_WORKSPACE_HOST) or _env("DATABRICKS_HOST")
    if host:
        if not host.startswith("http://") and not host.startswith("https://"):
            host = f"https://{host}"
        settings.workspace.host = host.rstrip("/")

    for action_id, env_names in (
        ("onboard", (ENV_ONBOARDING_JOB_ID, "ONBOARDING_JOB_ID", "DATABRICKS_ONBOARDING_JOB_ID", "JOB_ID")),
        ("validate", (ENV_VALIDATE_JOB_ID, "VALIDATE_JOB_ID", "DATABRICKS_VALIDATE_JOB_ID")),
    ):
        raw = None
        for env_name in env_names:
            raw = _env(env_name)
            if raw:
                break
        if raw and action_id in settings.actions:
            try:
                settings.actions[action_id].job_id = int(raw)
            except ValueError:
                raise ValueError(
                    f"{env_names[0]}={raw!r} is not a valid job id. It must be the numeric "
                    f"Databricks job id (bundle: ${{resources.jobs.<job>.id}})."
                )
        elif action_id in settings.actions and settings.actions[action_id].mode == "job" and settings.actions[action_id].job_id is None and os.environ.get(ENV_FAKE_DBX) == "1":
            settings.actions[action_id].job_id = 987654321098765

    for var_name, env_name in (("catalog", ENV_SPEC_CATALOG), ("env", ENV_SPEC_ENV)):
        raw = _env(env_name)
        if raw and var_name in settings.template_variables:
            settings.template_variables[var_name].default = raw

    for kind, env_name in (("volume", ENV_SPEC_VOLUME_ROOT), ("workspace", ENV_SPEC_WORKSPACE_ROOT)):
        raw = _env(env_name)
        if not raw:
            continue
        for root in settings.spec_storage.roots:
            if root.kind == kind:
                root.path = raw if raw.endswith("/") else raw + "/"

    return settings


def load_settings(config_path: Optional[Union[str, Path]] = None) -> AppSettings:
    """Load and validate config/index.json, then overlay deploy-time env overrides."""
    if config_path is None:
        config_env = os.environ.get(ENV_APP_CONFIG)
        if config_env:
            config_path = Path(config_env)
        else:
            config_path = Path(__file__).parent.parent / "config" / "index.json"
    else:
        config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"App config file not found at: {config_path.resolve()}")

    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # The brand is owned by branding/branding.json, not by this config file. Pydantic's
    # AppInfo.title default is only reached when the key is ABSENT, so a stale "title"
    # left in index.json would silently shadow APP_TITLE -- branding.json could say one
    # thing while the app served another, with nothing failing. Popping it here makes the
    # branding value authoritative regardless of what the config file happens to carry.
    if isinstance(data.get("app"), dict):
        data["app"].pop("title", None)

    settings = AppSettings.model_validate(data)
    settings.config_dir = config_path.parent
    return apply_env_overrides(settings)
