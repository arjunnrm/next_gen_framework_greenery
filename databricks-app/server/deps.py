"""
FastAPI Request Dependencies & Context Extraction (§3 & §10.1).
"""

from typing import Any, Dict, Optional
import uuid
from fastapi import Header, Request

from server.clients.dbx import get_workspace_client
from server.core.registry import RegistryManager
from server.settings import AppSettings, load_settings


_SETTINGS: Optional[AppSettings] = None
_REGISTRY: Optional[RegistryManager] = None


def get_app_settings() -> AppSettings:
    """Get or initialize singleton AppSettings."""
    global _SETTINGS
    if _SETTINGS is None:
        _SETTINGS = load_settings()
    return _SETTINGS


def get_registry_manager() -> RegistryManager:
    """Get or initialize singleton RegistryManager."""
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = RegistryManager()
    return _REGISTRY


def get_request_id(
    request: Request,
    x_request_id: Optional[str] = Header(None, alias="X-Request-Id")
) -> str:
    """Extract or mint a request ID and store on request.state."""
    req_id = x_request_id or f"req_{uuid.uuid4().hex[:12]}"
    request.state.request_id = req_id
    return req_id


def get_user_identity(
    x_forwarded_email: Optional[str] = Header(None, alias="X-Forwarded-Email"),
    x_forwarded_user: Optional[str] = Header(None, alias="X-Forwarded-User"),
    x_forwarded_preferred_username: Optional[str] = Header(None, alias="X-Forwarded-Preferred-Username")
) -> str:
    """Extract user identity from Databricks Apps forwarded headers."""
    return x_forwarded_email or x_forwarded_user or x_forwarded_preferred_username or "developer@internal.example.com"


def get_dbx_client(
    request: Request,
    x_forwarded_access_token: Optional[str] = Header(None, alias="X-Forwarded-Access-Token")
) -> Any:
    """Yield WorkspaceClient per request."""
    settings = get_app_settings()
    token = x_forwarded_access_token if settings.auth.mode == "obo" else None
    return get_workspace_client(token=token, host=settings.workspace.host)
