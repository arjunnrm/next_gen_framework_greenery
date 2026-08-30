"""
Actions and Onboarding Job Runner Router (§9.1 & §10.4).
Endpoints:
  POST /api/actions/{action_id}/run
  GET /api/actions/runs/{run_id}
  POST /api/actions/runs/{run_id}/cancel
"""

from datetime import datetime, timezone
import json
import time
from typing import Any, Dict, Optional
from fastapi import APIRouter, Depends, Body, Path, Request

from server.clients.files import FileManager
from server.clients.jobs import JobRunner
from server.core.registry import RegistryManager
from server.core.deserializer import as_spec_doc
from server.core.serializer import SpecSerializer
from server.core.validator import SpecValidator
from server.deps import get_app_settings, get_dbx_client, get_registry_manager, get_request_id, get_user_identity
from server.errors import AppException
from server.settings import AppSettings


router = APIRouter(prefix="/api/actions", tags=["Actions"])


@router.post("/{action_id}/run")
def trigger_action_run(
    action_id: str = Path(...),
    payload: Dict[str, Any] = Body(...),
    settings: AppSettings = Depends(get_app_settings),
    reg: RegistryManager = Depends(get_registry_manager),
    client: Any = Depends(get_dbx_client),
    user: str = Depends(get_user_identity),
    request_id: str = Depends(get_request_id)
) -> Dict[str, Any]:
    """Execute an action (e.g. validate, onboard) with server-side validation and staging."""
    # The React frontend posts the canonical framework spec; older callers post an
    # internal SpecDoc. as_spec_doc accepts either.
    spec_doc = as_spec_doc(payload.get("spec", {}), reg)
    confirmed = payload.get("confirmed", False)
    custom_params = payload.get("params", {})

    if action_id not in settings.actions:
        raise AppException(code="NOT_FOUND", message=f"Action '{action_id}' not found.", status_code=404)

    action = settings.actions[action_id]
    if action.confirm and not confirmed:
        raise AppException(
            code="VALIDATION_FAILED",
            message="Action requires explicit confirmation.",
            status_code=400
        )

    # 1. Server-side Serialization & In-App Validation
    serializer = SpecSerializer(reg)
    validator = SpecValidator(reg)

    val_res = validator.validate_spec(spec_doc)
    if not val_res["ok"]:
        raise AppException(
            code="VALIDATION_FAILED",
            message="Spec validation failed.",
            status_code=400,
            detail=json.dumps(val_res["errors"])
        )

    spec_dict = serializer.serialize_spec(spec_doc)
    spec_content, _ = serializer.render(spec_doc, fmt="json")

    # 2. Stage the spec file
    group_id = spec_dict.get("dataflow_group_id", "spec")
    ts = int(time.time())
    staged_filename = f"_runs/{action_id}/{user}/{ts}_{group_id}.json"
    file_mgr = FileManager(settings, client)

    staged_full_path = ""
    try:
        root_id = settings.spec_storage.default_root
        staged_full_path, _, _ = file_mgr.write_file(
            root_id=root_id,
            path=staged_filename,
            content=spec_content,
            overwrite=True
        )
    except Exception:
        # Fallback to inline or continue if staging fails on local mode
        if action.mode != "local":
            staged_full_path = f"/Volumes/{settings.template_variables['catalog'].default}/metaflow/onboarding_specs/{staged_filename}"

    # 3. Trigger action via JobRunner
    runner = JobRunner(settings, client)
    context_values = {
        "spec_path": staged_full_path,
        "spec_inline": spec_content,
        "dataflow_group_id": group_id,
        "env": settings.template_variables.get("env", type("O", (), {"default": "dev"})).default,
        "catalog": settings.template_variables.get("catalog", type("O", (), {"default": "metaflow"})).default,
        "user_email": user,
        "request_id": request_id
    }
    context_values.update(custom_params)

    return runner.run_action(action_id, spec_dict, context_values)


@router.get("/runs/{run_id}")
def get_run_status(
    run_id: str = Path(...),
    action_id: Optional[str] = None,
    settings: AppSettings = Depends(get_app_settings),
    client: Any = Depends(get_dbx_client)
) -> Dict[str, Any]:
    """Get live progress and state of an action run."""
    runner = JobRunner(settings, client)
    return runner.get_run_status(run_id, action_id=action_id)


@router.post("/runs/{run_id}/cancel")
def cancel_run(
    run_id: str = Path(...),
    settings: AppSettings = Depends(get_app_settings),
    client: Any = Depends(get_dbx_client)
) -> Dict[str, Any]:
    """Cancel an ongoing run."""
    runner = JobRunner(settings, client)
    return runner.cancel_run(run_id)
