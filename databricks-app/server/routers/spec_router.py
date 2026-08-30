"""
Spec Router (§9.1).
Endpoints:
  POST /api/spec/validate
  POST /api/spec/render
  POST /api/spec/import
  POST /api/spec/diff
"""

import json
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, Body, Request


from server.clients.files import FileManager
from server.core.deserializer import SpecDeserializer, as_spec_doc
from server.core.diff import SpecDiffer
from server.core.registry import RegistryManager
from server.core.serializer import SpecSerializer
from server.core.validator import SpecValidator
from server.deps import get_app_settings, get_dbx_client, get_registry_manager, get_request_id
from server.errors import AppException
from server.settings import AppSettings


router = APIRouter(prefix="/api/spec", tags=["Spec"])


@router.post("/validate")
def validate_spec(
    spec: Dict[str, Any] = Body(..., embed=True),
    reg: RegistryManager = Depends(get_registry_manager)
) -> Dict[str, Any]:
    """Perform in-app Layer 1 + Layer 2 validation and secret hygiene check.

    Accepts either an internal SpecDoc or a canonical framework spec.
    """
    validator = SpecValidator(reg)
    return validator.validate_spec(as_spec_doc(spec, reg))


@router.post("/render")
def render_spec(
    spec: Dict[str, Any] = Body(..., embed=True),
    format: str = Body("json", embed=True),
    reg: RegistryManager = Depends(get_registry_manager)
) -> Dict[str, Any]:
    """Serialize SpecDoc or raw spec dictionary to JSON or YAML text."""
    serializer = SpecSerializer(reg)
    deserializer = SpecDeserializer(reg)

    spec_doc = as_spec_doc(spec, reg)

    content, byte_count = serializer.render(spec_doc, fmt=format)
    return {
        "content": content,
        "format": format.lower(),
        "bytes": byte_count
    }



@router.post("/import")
def import_spec(
    payload: Dict[str, Any] = Body(...),
    settings: AppSettings = Depends(get_app_settings),
    reg: RegistryManager = Depends(get_registry_manager),
    client: Any = Depends(get_dbx_client)
) -> Dict[str, Any]:
    """Import a spec from either raw string content or a storage root path."""
    deserializer = SpecDeserializer(reg)
    content = payload.get("content")
    fmt = payload.get("format", "json")
    root_id = payload.get("root_id")
    path = payload.get("path")
    template_vars = payload.get("template_vars")

    if path and root_id:
        file_mgr = FileManager(settings, client)
        content, fmt, _ = file_mgr.read_file(root_id, path, template_vars=template_vars)

    if not content:
        raise AppException(code="VALIDATION_FAILED", message="Either 'content' or 'path' + 'root_id' must be provided.", status_code=400)

    spec_doc = deserializer.deserialize_spec(content, fmt=fmt)
    return {
        "spec": spec_doc,
        "unknown": spec_doc.get("meta", {}).get("unknown", []),
        "warnings": spec_doc.get("meta", {}).get("warnings", [])
    }


@router.post("/diff")
def diff_spec(
    payload: Dict[str, Any] = Body(...),
    settings: AppSettings = Depends(get_app_settings),
    reg: RegistryManager = Depends(get_registry_manager),
    client: Any = Depends(get_dbx_client)
) -> Dict[str, Any]:
    """Compare a new spec against an existing file or spec dictionary."""
    new_spec_doc = payload.get("spec", {})
    root_id = payload.get("root_id")
    path = payload.get("path")
    old_spec_raw = payload.get("old_spec")
    template_vars = payload.get("template_vars")

    serializer = SpecSerializer(reg)
    deserializer = SpecDeserializer(reg)

    new_serialized = serializer.serialize_spec(new_spec_doc)

    old_serialized: Dict[str, Any] = {}
    if path and root_id:
        file_mgr = FileManager(settings, client)
        content, fmt, _ = file_mgr.read_file(root_id, path, template_vars=template_vars)
        old_doc = deserializer.deserialize_spec(content, fmt=fmt)
        old_serialized = serializer.serialize_spec(old_doc)
    elif old_spec_raw:
        if "root" in old_spec_raw:
            old_serialized = serializer.serialize_spec(old_spec_raw)
        else:
            old_serialized = old_spec_raw

    return SpecDiffer.diff_specs(new_serialized, old_serialized)
