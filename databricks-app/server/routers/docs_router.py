"""
Documentation Router & Link Resolver (§5.6 & §11).
"""

from typing import Any, Dict, Optional
from fastapi import APIRouter, Depends, Query

from server.core.registry import RegistryManager
from server.deps import get_app_settings, get_registry_manager
from server.settings import AppSettings


router = APIRouter(prefix="/api/docs", tags=["Docs"])


@router.get("/resolve")
def resolve_doc_link(
    path: str = Query(...),
    settings: AppSettings = Depends(get_app_settings),
    reg: RegistryManager = Depends(get_registry_manager)
) -> Dict[str, str]:
    """Resolve an attribute path to a full documentation URL and anchor."""
    docs_map = reg.docs
    base_url = settings.docs.base_url.rstrip("/")
    if settings.docs.mode == "external":
        base_url = settings.docs.external_base_url.rstrip("/")

    anchors = docs_map.get("anchors", {})

    # 1. Exact match
    if path in anchors:
        entry = anchors[path]
        page = docs_map.get("pages", {}).get(entry.get("page"), "")
        anchor = entry.get("anchor", "")
        return {"url": f"{base_url}/{page}{anchor}"}

    # 2. Longest prefix match
    matched_prefix = ""
    for k in sorted(anchors.keys(), key=len, reverse=True):
        if path.startswith(k):
            matched_prefix = k
            break

    if matched_prefix:
        entry = anchors[matched_prefix]
        page = docs_map.get("pages", {}).get(entry.get("page"), "")
        anchor = entry.get("anchor", "")
        return {"url": f"{base_url}/{page}{anchor}"}

    # 3. Default fallback
    default_page = docs_map.get("pages", {}).get(docs_map.get("default_page", "attribute_reference"), "")
    return {"url": f"{base_url}/{default_page}"}
