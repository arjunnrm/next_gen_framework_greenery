"""
Actions and Onboarding Job Runner Router (§9.1 & §10.4).
Endpoints:
  POST /api/actions/{action_id}/run
  GET /api/actions/runs/{run_id}
  POST /api/actions/runs/{run_id}/cancel
"""


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


import logging

logger = logging.getLogger("flowx_app")


router = APIRouter(prefix="/api/actions", tags=["Actions"])


def _template_default(settings: AppSettings, name: str, fallback: str) -> str:
    var = settings.template_variables.get(name)
    return getattr(var, "default", fallback) if var else fallback



@router.get("/{action_id}/parameters")
def get_action_parameters(
    action_id: str = Path(...),
    dataflow_group_id: str = "",
    settings: AppSettings = Depends(get_app_settings),
    client: Any = Depends(get_dbx_client),
    user: str = Depends(get_user_identity),
    request_id: str = Depends(get_request_id),
) -> Dict[str, Any]:
    """Every parameter this action will send to its job, resolved and ready to be edited.

    The UI asks for this before running so the operator can see and change each value
    instead of discovering after the fact what the app decided on their behalf.

    It also compares the configured `parameter_map` against the parameters the job really
    declares. Databricks rejects `run_now` outright when `job_parameters` contains a name
    the job does not declare, so a drifted map fails the whole run with an error that
    names neither the app nor the offending key — `unknown_to_job` says it plainly.
    """
    if action_id not in settings.actions:
        raise AppException(code="NOT_FOUND", message=f"Action '{action_id}' not found.", status_code=404)

    action = settings.actions[action_id]
    catalog = _template_default(settings, "catalog", "flowx")
    env = _template_default(settings, "env", "dev")

    # Placeholder values for the two context keys that only exist once a spec is staged,
    # so the operator sees the shape of what will be sent rather than an empty box.
    context_values = {
        "spec_path": "<staged at run time>",
        "spec_file_path": "<staged at run time>",
        "spec_inline": "<the spec document>",
        "dataflow_group_id": dataflow_group_id or "<from the spec>",
        "env": env,
        "environment": env,
        "catalog": catalog,
        "user_email": user,
        "action_type": "CREATE",
        "request_id": request_id,
    }

    # Introspect the Databricks job's declared parameters so we can flag mismatches
    declared = None
    if action.mode == "job" and action.job_id:
        try:
            job = client.jobs.get(action.job_id)
            params = getattr(getattr(job, "settings", None), "parameters", None) or []
            declared = {p.name: ("" if p.default is None else str(p.default)) for p in params}
        except Exception:
            declared = None

    parameters = []
    for name, source in action.parameter_map.items():
        is_ref = isinstance(source, str) and source.startswith("$")
        ref = source[1:] if is_ref else ""
        parameters.append({
            "name": name,
            "value": str(context_values.get(ref, "")) if is_ref else str(source),
            "source": source,
            # A $spec_path is produced by the server when it stages the file; letting
            # someone hand-edit it would point the job at a file that is not the spec
            # they are looking at.
            "editable": ref not in ("spec_path", "spec_file_path", "spec_inline", "request_id"),
            "bound_to": ref if is_ref else None,
            "job_default": (declared or {}).get(name),
            "declared_by_job": None if declared is None else (name in declared),
        })

    unknown_to_job = [] if declared is None else [p["name"] for p in parameters if p["name"] not in declared]
    missing_from_map = [] if declared is None else [n for n in declared if n not in action.parameter_map]

    return {
        "action_id": action_id,
        "label": action.label,
        "mode": action.mode,
        "job_id": action.job_id,
        "confirm": action.confirm,
        "confirm_text": action.confirm_text,
        "parameters": parameters,
        "job_declared": None if declared is None else sorted(declared),
        "unknown_to_job": unknown_to_job,
        "missing_from_map": missing_from_map,
    }


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
    """Execute an action (e.g. validate, onboard) with server-side validation, staging, and parameter forwarding."""
    # The React frontend posts the canonical framework spec; older callers post an
    # internal SpecDoc. as_spec_doc accepts either.
    spec_doc = as_spec_doc(payload.get("spec", {}), reg)
    confirmed = payload.get("confirmed", False)
    custom_params = payload.get("params") or payload.get("context") or {}
    if not isinstance(custom_params, dict):
        raise AppException(
            code="VALIDATION_FAILED",
            message="'params' must be an object of key -> value.",
            status_code=400,
        )

    # Values the operator typed into the run panel or API request, keyed by *job parameter name*.
    job_parameter_overrides = payload.get("job_parameters") or payload.get("overrides") or {}
    if not isinstance(job_parameter_overrides, dict):
        raise AppException(
            code="VALIDATION_FAILED",
            message="'job_parameters' must be an object of parameter name -> value.",
            status_code=400,
        )

    notebook_params = payload.get("notebook_params")
    if notebook_params is not None and not isinstance(notebook_params, dict):
        raise AppException(
            code="VALIDATION_FAILED",
            message="'notebook_params' must be an object of parameter name -> value.",
            status_code=400,
        )

    python_params = payload.get("python_params")
    if python_params is not None and not isinstance(python_params, list):
        raise AppException(
            code="VALIDATION_FAILED",
            message="'python_params' must be a list of string arguments.",
            status_code=400,
        )

    python_named_params = payload.get("python_named_params")
    if python_named_params is not None and not isinstance(python_named_params, dict):
        raise AppException(
            code="VALIDATION_FAILED",
            message="'python_named_params' must be an object of parameter name -> value.",
            status_code=400,
        )

    sql_params = payload.get("sql_params")
    jar_params = payload.get("jar_params")
    idempotency_token = payload.get("idempotency_token")

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
        err_msgs = [e.get("message", str(e)) for e in val_res.get("errors", [])]
        summary_msg = f"Spec validation failed: {'; '.join(err_msgs[:3])}" if err_msgs else "Spec validation failed."
        raise AppException(
            code="VALIDATION_FAILED",
            message=summary_msg,
            status_code=400,
            detail=json.dumps(val_res["errors"])
        )

    spec_dict = serializer.serialize_spec(spec_doc)
    spec_content, _ = serializer.render(spec_doc, fmt="json")

    # 2. Spec file staging or use provided saved spec path
    group_id = spec_dict.get("dataflow_group_id", "spec")
    provided_spec_path = (
        payload.get("spec_path")
        or custom_params.get("spec_path")
        or job_parameter_overrides.get("spec_path")
    )

    staged_full_path = ""
    if provided_spec_path and isinstance(provided_spec_path, str) and provided_spec_path.strip():
        staged_full_path = provided_spec_path.strip()
        logger.info(
            f"Using user-specified saved spec path for action '{action_id}': '{staged_full_path}'.",
            extra={"event": "spec_using_provided_path", "action_id": action_id, "path": staged_full_path, "user": user}
        )
    else:
        ts = int(time.time())
        staged_filename = f"_runs/{action_id}/{user}/{ts}_{group_id}.json"
        file_mgr = FileManager(settings, client)

        try:
            root_id = settings.spec_storage.default_root
            staged_full_path, _, _ = file_mgr.write_file(
                root_id=root_id,
                path=staged_filename,
                content=spec_content,
                overwrite=True
            )
            logger.info(
                f"Successfully staged spec file for action '{action_id}' to '{staged_full_path}'.",
                extra={"event": "spec_staging_success", "action_id": action_id, "path": staged_full_path, "user": user}
            )
        except Exception as ex:
            if action.mode == "job":
                logger.error(
                    f"Failed to stage spec file for action '{action_id}': {ex}",
                    extra={"event": "spec_staging_failed", "action_id": action_id, "user": user, "error": str(ex)}
                )
                raise AppException(
                    code="UPSTREAM_ERROR",
                    message=f"Failed to stage spec file to '{settings.spec_storage.default_root}': {ex}",
                    status_code=502
                )
            else:
                staged_full_path = f"/Volumes/{settings.template_variables['catalog'].default}/flowx/onboarding_specs/{staged_filename}"

    # 3. Trigger action via JobRunner using Databricks SDK with user OBO token
    runner = JobRunner(settings, client)

    # Extract target catalog from spec flows if explicit
    spec_catalog = None
    for flow_key in ("ingestion_flows", "transformation_flows", "reconciliation_flows"):
        for flow in spec_dict.get(flow_key, []):
            cat = flow.get("target_catalog")
            if cat and isinstance(cat, str) and "{{" not in cat:
                spec_catalog = cat.strip()
                break
        if spec_catalog:
            break

    catalog_val = (
        custom_params.get("catalog")
        or job_parameter_overrides.get("catalog")
        or spec_catalog
        or _template_default(settings, "catalog", "flowx")
    )
    env_val = (
        custom_params.get("env")
        or custom_params.get("environment")
        or job_parameter_overrides.get("env")
        or job_parameter_overrides.get("environment")
        or _template_default(settings, "env", "dev")
    )
    action_type_val = (
        custom_params.get("action_type")
        or job_parameter_overrides.get("action_type")
        or "CREATE"
    )

    context_values = {
        "spec_path": staged_full_path,
        "spec_file_path": staged_full_path,
        "spec_inline": spec_content,
        "dataflow_group_id": group_id,
        "env": env_val,
        "environment": env_val,
        "catalog": catalog_val,
        "user_email": user,
        "action_type": action_type_val,
        "request_id": request_id
    }
    context_values.update(custom_params)

    return runner.run_action(
        action_id=action_id,
        spec_dict=spec_dict,
        context_values=context_values,
        overrides=job_parameter_overrides,
        job_parameters=payload.get("job_parameters"),
        notebook_params=notebook_params,
        python_params=python_params,
        python_named_params=python_named_params,
        sql_params=sql_params,
        jar_params=jar_params,
        idempotency_token=idempotency_token,
        job_id_override=payload.get("job_id") or custom_params.get("job_id"),
    )


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
