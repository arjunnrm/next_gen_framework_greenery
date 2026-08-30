"""
Debug Endpoints Router (§5.8 & §15).
Endpoints:
  GET /api/debug/config?path=
  POST /api/debug/predicate
"""

from typing import Any, Dict, Optional
from fastapi import APIRouter, Depends, Body, Query

from server.core.predicates import evaluate_predicate, resolve_path_value
from server.core.registry import RegistryManager
from server.deps import get_app_settings, get_registry_manager
from server.settings import AppSettings


router = APIRouter(prefix="/api/debug", tags=["Debug"])


@router.get("/config")
def debug_config(
    path: str = Query(""),
    settings: AppSettings = Depends(get_app_settings),
    reg: RegistryManager = Depends(get_registry_manager)
) -> Dict[str, Any]:
    """Inspect resolved config node and its metadata."""
    if not path:
        return {
            "app": settings.app.model_dump(),
            "meta": reg.meta,
            "counts": {
                "documented_attributes": reg.count_documented_attributes(),
                "rules": len(reg.rules)
            }
        }

    # Search for field definition across registries
    for kind, fmap in reg.field_maps.items():
        if path in fmap:
            return {
                "flow_kind": kind,
                "path": path,
                "field_descriptor": fmap[path]
            }

    return {"error": f"Node '{path}' not found in registry."}


@router.post("/predicate")
def debug_predicate(
    payload: Dict[str, Any] = Body(...)
) -> Dict[str, Any]:
    """Evaluate a predicate and return detailed trace."""
    pred = payload.get("predicate")
    ctx = payload.get("context", {})
    result = evaluate_predicate(pred, ctx)
    return {
        "result": result,
        "predicate": pred,
        "context": ctx
    }
