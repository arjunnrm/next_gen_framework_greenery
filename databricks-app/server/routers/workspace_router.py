"""
Workspace & Volume Storage Router (§9.1 & §10).
Endpoints:
  GET /api/workspace/access
  GET /api/workspace/list
  GET /api/workspace/read
  POST /api/workspace/write
"""

import json
from typing import Any, Dict, List, Optional

import yaml
from fastapi import APIRouter, Depends, Body, Query

from server.clients.access import AccessChecker
from server.clients.files import FileManager
from server.core.deserializer import as_spec_doc
from server.core.registry import RegistryManager
from server.core.validator import SpecValidator
from server.deps import get_app_settings, get_dbx_client, get_registry_manager, get_user_identity
from server.errors import AppException
from server.settings import AppSettings


router = APIRouter(prefix="/api/workspace", tags=["Workspace"])


def _parse_template_vars(raw: Optional[str]) -> Optional[Dict[str, str]]:
    """Decode the optional `template_vars` query parameter.

    /write has always accepted template variables in its JSON body; /list and /read are
    GETs and had no way to receive them, so a caller working in a non-default catalog
    could write to one directory and then be unable to list or read it back. Accepting a
    small JSON object here closes that asymmetry.
    """
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except Exception as ex:
        raise AppException(
            code="VALIDATION_FAILED",
            message=f"'template_vars' must be a JSON object: {ex}",
            status_code=400,
        )
    if not isinstance(parsed, dict):
        raise AppException(
            code="VALIDATION_FAILED",
            message="'template_vars' must be a JSON object of name -> value.",
            status_code=400,
        )
    return {str(k): str(v) for k, v in parsed.items()}


@router.get("/access")
def get_access_report(
    root_id: Optional[str] = Query(None),
    template_vars: Optional[str] = Query(None),
    settings: AppSettings = Depends(get_app_settings),
    client: Any = Depends(get_dbx_client),
    user: str = Depends(get_user_identity)
) -> Dict[str, Any]:
    """Run non-destructive permission preflight checks."""
    checker = AccessChecker(settings, client, user_email=user)
    return checker.run_checks(root_id=root_id, template_vars=_parse_template_vars(template_vars))


@router.get("/list")
def list_workspace_files(
    root_id: Optional[str] = Query(None),
    prefix: str = Query(""),
    template_vars: Optional[str] = Query(None),
    settings: AppSettings = Depends(get_app_settings),
    client: Any = Depends(get_dbx_client)
) -> Dict[str, Any]:
    """List spec files in Volume or Workspace storage."""
    file_mgr = FileManager(settings, client)
    r_id = root_id or settings.spec_storage.default_root
    tvars = _parse_template_vars(template_vars)
    entries = file_mgr.list_files(r_id, prefix=prefix, template_vars=tvars)
    root = file_mgr._get_root(r_id)
    return {
        "entries": entries,
        "root_id": r_id,
        "root_path": file_mgr.resolve_root_path(root, tvars),
    }


@router.get("/read")
def read_workspace_file(
    path: str = Query(...),
    root_id: Optional[str] = Query(None),
    template_vars: Optional[str] = Query(None),
    settings: AppSettings = Depends(get_app_settings),
    client: Any = Depends(get_dbx_client)
) -> Dict[str, Any]:
    """Read a spec file from storage root."""
    file_mgr = FileManager(settings, client)
    r_id = root_id or settings.spec_storage.default_root
    content, fmt, etag = file_mgr.read_file(r_id, path, template_vars=_parse_template_vars(template_vars))
    return {
        "content": content,
        "format": fmt,
        "etag": etag,
        "path": path
    }


@router.post("/write")
def write_workspace_file(
    payload: Dict[str, Any] = Body(...),
    settings: AppSettings = Depends(get_app_settings),
    client: Any = Depends(get_dbx_client)
) -> Dict[str, Any]:
    """Write or overwrite a spec file in storage root."""
    file_mgr = FileManager(settings, client)
    root_id = payload.get("root_id") or settings.spec_storage.default_root
    path = payload.get("path", "")
    content = payload.get("content", "")
    overwrite = payload.get("overwrite", False)
    expected_etag = payload.get("etag")
    template_vars = payload.get("template_vars")

    full_path, byte_count, etag = file_mgr.write_file(
        root_id=root_id,
        path=path,
        content=content,
        overwrite=overwrite,
        expected_etag=expected_etag,
        template_vars=template_vars
    )

    return {
        "path": full_path,
        "bytes": byte_count,
        "etag": etag
    }


@router.post("/validate")
def validate_storage_file(
    payload: Dict[str, Any] = Body(...),
    settings: AppSettings = Depends(get_app_settings),
    reg: RegistryManager = Depends(get_registry_manager),
    client: Any = Depends(get_dbx_client)
) -> Dict[str, Any]:
    """Read a spec from a user-supplied Volume or Workspace path, parse it, and validate it.

    This is what backs the "check this path before I open it" affordance in the UI. It
    is deliberately non-destructive: it reads through the same FileManager as everything
    else (so path traversal and extension rules still apply), parses JSON or YAML, and
    then runs the ordinary spec validator.

    A parse failure and a validation failure are reported differently: the first means the
    file is not a spec at all, the second means it is a spec with problems. Both return 200
    with `ok: false` rather than an error status, because "this file has issues" is a normal
    answer to the question the UI is asking, not a transport failure.
    """
    root_id = payload.get("root_id") or settings.spec_storage.default_root
    path = (payload.get("path") or "").strip()
    template_vars = payload.get("template_vars")

    if not path:
        raise AppException(code="VALIDATION_FAILED", message="A 'path' is required.", status_code=400)

    file_mgr = FileManager(settings, client)
    content, fmt, etag = file_mgr.read_file(root_id, path, template_vars=template_vars)

    # 1. Parse
    try:
        raw = yaml.safe_load(content) if fmt in ("yaml", "yml") else json.loads(content)
    except Exception as ex:
        return {
            "ok": False,
            "stage": "parse",
            "path": path,
            "format": fmt,
            "bytes": len(content.encode("utf-8")),
            "etag": etag,
            "parse_error": str(ex),
            "errors": [],
            "warnings": [],
            "summary": {},
        }

    if not isinstance(raw, dict):
        return {
            "ok": False,
            "stage": "parse",
            "path": path,
            "format": fmt,
            "bytes": len(content.encode("utf-8")),
            "etag": etag,
            "parse_error": f"Top level of the document is {type(raw).__name__}, expected an object.",
            "errors": [],
            "warnings": [],
            "summary": {},
        }

    # 2. Validate — accepts the canonical framework spec or an internal SpecDoc
    result = SpecValidator(reg).validate_spec(as_spec_doc(raw, reg))

    return {
        "ok": bool(result.get("ok")),
        "stage": "validate",
        "path": path,
        "format": fmt,
        "bytes": len(content.encode("utf-8")),
        "etag": etag,
        "parse_error": "",
        "errors": result.get("errors", []),
        "warnings": result.get("warnings", []),
        "summary": {
            "dataflow_group_id": raw.get("dataflow_group_id", ""),
            "ingestion_flows": len(raw.get("ingestion_flows", []) or []),
            "transformation_flows": len(raw.get("transformation_flows", []) or []),
            "reconciliation_flows": len(raw.get("reconciliation_flows", []) or []),
            "observability": len(raw.get("observability", []) or []),
        },
    }
