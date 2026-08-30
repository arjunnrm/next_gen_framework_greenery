"""
Unified Error Envelope for Metaflow Onboarding App (§9.2).
Error Codes:
  CONFIG_INVALID | VALIDATION_FAILED | NOT_FOUND | PERMISSION_DENIED |
  UPSTREAM_ERROR | RATE_LIMITED | TIMEOUT | PAYLOAD_TOO_LARGE |
  UNSUPPORTED_FORMAT | INTERNAL
"""

from typing import Any, Dict, Optional
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse


class AppException(HTTPException):
    """Base application exception carrying structured error envelope."""

    def __init__(
        self,
        code: str,
        message: str,
        status_code: int = 400,
        detail: Optional[str] = None,
        field_path: Optional[str] = None,
        docs_url: Optional[str] = None,
    ):
        super().__init__(status_code=status_code, detail=message)
        self.code = code
        self.message = message
        self.error_detail = detail
        self.field_path = field_path
        self.docs_url = docs_url


def format_error_response(
    request: Request,
    code: str,
    message: str,
    status_code: int = 400,
    detail: Optional[str] = None,
    field_path: Optional[str] = None,
    docs_url: Optional[str] = None,
) -> JSONResponse:
    """Construct JSON response with error envelope matching §9.2."""
    request_id = getattr(request.state, "request_id", "req_unknown")
    envelope = {
        "error": {
            "code": code,
            "message": message,
            "detail": detail,
            "field_path": field_path,
            "request_id": request_id,
            "docs_url": docs_url or "/docs/01_platform_architecture/"
        }
    }
    return JSONResponse(status_code=status_code, content=envelope, headers={"X-Request-Id": request_id})
