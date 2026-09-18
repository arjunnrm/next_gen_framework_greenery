import logging
import os
from typing import Any, Optional, Tuple
import uuid
from fastapi import Header, Request

from server.clients.dbx import get_workspace_client
from server.core.registry import RegistryManager
from server.errors import AppException
from server.branding_generated import env_var
from server.settings import ENV_FAKE_DBX, AppSettings, load_settings

# Test-only hook, set solely by tests/test_obo_and_job_execution.py, read solely here.
ENV_TEST_OBO_STRICT = env_var("TEST_OBO_STRICT")


logger = logging.getLogger("flowx_app")

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
    if isinstance(x_request_id, str) and x_request_id.strip():
        req_id = x_request_id.strip()
    else:
        req_id = getattr(getattr(request, "state", None), "request_id", None)
        if not isinstance(req_id, str) or not req_id.strip():
            req_id = request.headers.get("X-Request-Id") if hasattr(request, "headers") else None
        if not isinstance(req_id, str) or not req_id.strip():
            req_id = f"req_{uuid.uuid4().hex[:12]}"

    if hasattr(request, "state"):
        request.state.request_id = req_id
    return req_id


def get_user_identity(
    x_forwarded_email: Optional[str] = Header(None, alias="X-Forwarded-Email"),
    x_forwarded_user: Optional[str] = Header(None, alias="X-Forwarded-User"),
    x_forwarded_preferred_username: Optional[str] = Header(None, alias="X-Forwarded-Preferred-Username")
) -> str:
    """Extract user identity from Databricks Apps forwarded headers."""
    email = x_forwarded_email if isinstance(x_forwarded_email, str) and x_forwarded_email.strip() else None
    user = x_forwarded_user if isinstance(x_forwarded_user, str) and x_forwarded_user.strip() else None
    preferred = x_forwarded_preferred_username if isinstance(x_forwarded_preferred_username, str) and x_forwarded_preferred_username.strip() else None
    return email or user or preferred or "developer@internal.example.com"


def _extract_auth_token(request: Request) -> Tuple[Optional[str], Optional[str]]:
    """Extract OBO bearer token from the x-forwarded-access-token header.

    Databricks Apps forwards the signed-in user's OAuth access token in this
    header.  No other header source is supported in production.
    """
    if not hasattr(request, "headers"):
        return None, None
    hdr_token = (
        request.headers.get("x-forwarded-access-token")
        or request.headers.get("X-Forwarded-Access-Token")
    )
    if hdr_token and isinstance(hdr_token, str) and hdr_token.strip():
        return hdr_token.strip(), "x-forwarded-access-token"
    return None, None


def get_dbx_client(request: Request) -> Any:
    """Yield authenticated WorkspaceClient per request with OBO delegation."""
    settings = get_app_settings()
    user_email = request.headers.get("X-Forwarded-Email") if hasattr(request, "headers") else None
    user_name = request.headers.get("X-Forwarded-User") if hasattr(request, "headers") else None
    preferred = request.headers.get("X-Forwarded-Preferred-Username") if hasattr(request, "headers") else None
    user = get_user_identity(user_email, user_name, preferred)
    req_id = get_request_id(request)

    token, token_source = _extract_auth_token(request)

    if settings.auth.mode == "obo":
        if token:
            logger.debug(
                f"OBO token acquired from {token_source} for user {user}.",
                extra={
                    "request_id": req_id,
                    "user": user,
                    "event": "obo_token_acquired",
                    "token_source": token_source,
                    "auth_mode": "obo"
                }
            )
        else:
            if settings.auth.fallback_to_sp:
                logger.info(
                    f"No OBO token found in request headers for user {user}; falling back to service principal credentials.",
                    extra={
                        "request_id": req_id,
                        "user": user,
                        "event": "obo_token_fallback_to_sp",
                        "auth_mode": "obo"
                    }
                )
            elif os.environ.get(ENV_FAKE_DBX) == "1" and not os.environ.get(ENV_TEST_OBO_STRICT):
                logger.debug(f"{ENV_FAKE_DBX} active: permitting mock client without OBO token.")
            else:
                logger.warning(
                    f"User '{user}' does not have access: OBO token missing from request headers.",
                    extra={
                        "request_id": req_id,
                        "user": user,
                        "event": "obo_token_missing",
                        "auth_mode": "obo"
                    }
                )
                raise AppException(
                    code="PERMISSION_DENIED",
                    message=f"User '{user}' does not have access: on-behalf-of authentication token is required.",
                    status_code=403
                )
    else:
        token = None

    return get_workspace_client(token=token, host=settings.workspace.host, user=user)

