"""
Config, Registry, and Templates Router (§9.1).
Endpoints:
  GET /api/health
  GET /api/config
  GET /api/templates
  GET /api/templates/{id}
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, Request

from server.clients.files import resolve_template_placeholders
from server.core.registry import RegistryManager
from server.core.templates import load_template_body, scan_templates
from server.deps import get_app_settings, get_registry_manager, get_request_id, get_user_identity
from server.errors import AppException
from server.settings import AppSettings


router = APIRouter(prefix="/api", tags=["Config"])


def _load_attribute_knowledge(settings: AppSettings) -> Dict[str, Any]:
    """Knowledge overlay powering the attribute inspector.

    Optional: a missing or unparseable file degrades to an empty overlay, and the
    inspector falls back to the registry's own field descriptions. It is never worth
    failing a config request over supplementary help text.
    """
    kb_file = settings.config_dir / "attribute_knowledge.json"
    if not kb_file.exists():
        return {"attributes": {}, "defaults": {}, "reference_index": {}}
    try:
        return json.loads(kb_file.read_text(encoding="utf-8"))
    except Exception:
        return {"attributes": {}, "defaults": {}, "reference_index": {}}


def _resolved_spec_storage(settings: AppSettings) -> Dict[str, Any]:
    """spec_storage with every root path's template placeholders already substituted.

    The SPA seeds its "save to" directory box from `roots[].path` and posts that string
    back to /api/storage/write verbatim. Serving the raw `/Volumes/{{catalog}}/...` here
    meant every save and every open addressed a directory literally named `{{catalog}}`,
    which is why neither worked against a real workspace.

    `template_path` keeps the unsubstituted form for anything that needs to show or
    re-resolve the template; `path` is the usable one.
    """
    storage = settings.spec_storage.model_dump()
    for root in storage.get("roots", []):
        raw = root.get("path", "")
        root["template_path"] = raw
        root["path"] = resolve_template_placeholders(settings, raw)
    return storage


@router.get("/health")
def get_health(
    settings: AppSettings = Depends(get_app_settings),
    reg: RegistryManager = Depends(get_registry_manager),
    user: str = Depends(get_user_identity)
) -> Dict[str, Any]:
    """Health check reporting config status, version, and user identity."""
    template_errors: List[str] = []
    templates_index = settings.config_dir.parent / "templates" / "index.json"
    if templates_index.exists():
        try:
            with open(templates_index, "r", encoding="utf-8") as f:
                tdata = json.load(f)
                for t in tdata.get("templates", []):
                    tfile = settings.config_dir.parent / "templates" / t.get("file", "")
                    if not tfile.exists():
                        template_errors.append(f"Template '{t.get('id')}' file missing: {t.get('file')}")
        except Exception as ex:
            template_errors.append(f"Failed to parse templates/index.json: {str(ex)}")

    return {
        "status": "healthy",
        "framework_version": settings.app.framework_version,
        "config_ok": True,
        "template_errors": template_errors,
        "auth_mode": settings.auth.mode,
        "user": user
    }


@router.get("/config")
def get_resolved_config(
    settings: AppSettings = Depends(get_app_settings),
    reg: RegistryManager = Depends(get_registry_manager)
) -> Dict[str, Any]:
    """Return full resolved config payload (secrets stripped)."""
    # Templates are discovered from the directory, not from a checked-in list, so the
    # single config fetch the SPA makes on load already reflects anything dropped in.
    # The attribute knowledge overlay is deliberately NOT inlined here: it is ~113 KB
    # and only needed once someone opens the inspector. The SPA fetches it lazily from
    # /api/attribute-knowledge, keeping this initial payload small.
    templates = scan_templates(settings.config_dir.parent / "templates")

    template_variables = {k: v.model_dump() for k, v in settings.template_variables.items()}
    spec_storage = _resolved_spec_storage(settings)
    actions = {k: v.model_dump() for k, v in settings.actions.items()}
    features = settings.features.model_dump()
    limits = settings.limits.model_dump()

    onboarding_job_id = None
    if settings.actions.get("onboard") and settings.actions.get("onboard").job_id:
        onboarding_job_id = settings.actions.get("onboard").job_id
    if not onboarding_job_id:
        import os
        for env_k in ("FLOWX_ONBOARDING_JOB_ID", "ONBOARDING_JOB_ID", "DATABRICKS_ONBOARDING_JOB_ID", "JOB_ID"):
            val = os.environ.get(env_k, "").strip()
            if val and val.isdigit():
                onboarding_job_id = int(val)
                if "onboard" in actions:
                    actions["onboard"]["job_id"] = onboarding_job_id
                break

    # The React frontend (web/src/Builder.jsx::loadConfig) reads its storage roots
    # and action metadata from `app.spec_storage.roots` and `app.actions`, and then
    # keeps `cfg.app` as its whole config object. The original SPA read the same
    # values from the top level. Emit both: `app` gains the nested copies while every
    # top-level key stays exactly where it was, so neither consumer has to change.
    app_cfg = settings.app.model_dump()
    app_cfg.update({
        "features": features,
        "limits": limits,
        "template_variables": template_variables,
        "spec_storage": spec_storage,
        "actions": actions,
        "docs": settings.docs.model_dump(),
        "onboarding_job_id": onboarding_job_id,
    })

    return {
        "app": app_cfg,
        "onboarding_job_id": onboarding_job_id,
        "workspace": {
            "host": settings.workspace.host,
            "run_url_template": settings.workspace.run_url_template,
            "pipeline_url_template": settings.workspace.pipeline_url_template
        },
        "template_variables": template_variables,
        "spec_storage": spec_storage,
        "actions": actions,
        "docs": settings.docs.model_dump(),
        "features": features,
        "limits": limits,
        "registry": reg.registries,
        "phases": reg.phases,
        "docs_map": reg.docs,
        # Per-attribute wiki anchors, so the attribute inspector can link straight
        # to an attribute's own heading without a round trip to /api/docs/resolve.
        "docs_index": reg.docs_index,
        "templates": templates,
        "theme": reg.theme
    }


@router.get("/templates")
def list_templates(settings: AppSettings = Depends(get_app_settings)) -> Dict[str, Any]:
    """List every template discovered under templates/.

    The catalogue is built by scanning the directory on each call, so a file dropped
    into templates/ (or edited in place) shows up without a restart, a rebuild, or an
    index.json entry.
    """
    root = settings.config_dir.parent / "templates"
    templates = scan_templates(root)
    return {
        "schema_version": "2.0",
        "root": str(root),
        "count": len(templates),
        "templates": templates,
    }


@router.get("/templates/{template_id}")
def get_template(template_id: str, settings: AppSettings = Depends(get_app_settings)) -> Dict[str, Any]:
    """Get one discovered template's metadata and its raw JSON body."""
    root = settings.config_dir.parent / "templates"
    entry = next((t for t in scan_templates(root) if t["id"] == template_id), None)
    if not entry:
        raise AppException(code="NOT_FOUND", message=f"Template '{template_id}' not found.", status_code=404)

    body = load_template_body(root, entry["file"])
    if body is None:
        raise AppException(
            code="NOT_FOUND",
            message=f"Template file '{entry['file']}' could not be read.",
            status_code=404,
        )

    return {"id": template_id, "meta": entry, "spec": body}


@router.get("/attribute-knowledge")
def get_attribute_knowledge(settings: AppSettings = Depends(get_app_settings)) -> Dict[str, Any]:
    """The attribute inspector's knowledge overlay, on its own.

    Served separately as well as inside /api/config so the file can be edited and
    re-fetched without reloading the whole configuration.
    """
    kb = _load_attribute_knowledge(settings)
    return {
        "schema_version": kb.get("schema_version", "1.0"),
        "count": len(kb.get("attributes", {})),
        "reference_index": kb.get("reference_index", {}),
        "defaults": kb.get("defaults", {}),
        "attributes": kb.get("attributes", {}),
    }
