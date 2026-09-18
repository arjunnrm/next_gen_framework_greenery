"""
Main FastAPI Application for the Metaflow Onboarding App (§3 & §9).
Serves /api/* endpoints, /docs/* static/proxy documentation, and the built SPA.
"""

from contextlib import asynccontextmanager
from pathlib import Path
import sys
import time
import uuid

# Ensure app root is in sys.path
_APP_ROOT = Path(__file__).resolve().parent.parent
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from server.branding_generated import APP_RUNNING_MESSAGE, APP_TITLE
from server.deps import get_app_settings, get_registry_manager
from server.errors import AppException, format_error_response
from server.logging_setup import setup_logging
from server.routers import (
    config_router,
    debug_router,
    docs_router,
    onboard_router,
    spec_router,
    storage_router,
    workspace_router,
)


logger = setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup initialization: load settings, verify registry integrity, log banner."""
    settings = get_app_settings()
    reg = get_registry_manager()
    logger.info(
        f"Starting {settings.app.title} (v{settings.app.framework_version}) in {settings.app.environment_label} mode.",
        extra={"event": "app_startup", "user": "system"}
    )
    reg.verify_integrity()
    yield
    logger.info(f"{APP_TITLE} App shutdown complete.", extra={"event": "app_shutdown"})


app = FastAPI(
    title=f"{APP_TITLE} App",
    version="1.3.0",
    docs_url="/api/swagger",
    redoc_url=None,
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def request_context_middleware(request: Request, call_next):
    """Track request ID and log duration."""
    req_id = request.headers.get("X-Request-Id") or f"req_{uuid.uuid4().hex[:12]}"
    request.state.request_id = req_id
    start_time = time.time()

    response = await call_next(request)
    duration_ms = round((time.time() - start_time) * 1000, 2)
    response.headers["X-Request-Id"] = req_id

    # Log API requests
    if request.url.path.startswith("/api"):
        user = request.headers.get("X-Forwarded-Email") or request.headers.get("X-Forwarded-User") or "system"
        logger.info(
            f"{request.method} {request.url.path} -> {response.status_code} ({duration_ms}ms)",
            extra={
                "request_id": req_id,
                "user": user,
                "event": "api_request",
                "duration_ms": duration_ms,
                "outcome": "success" if response.status_code < 400 else "error"
            }
        )

    return response


# Exception Handlers
@app.exception_handler(AppException)
async def app_exception_handler(request: Request, exc: AppException):
    return format_error_response(
        request=request,
        code=exc.code,
        message=exc.message,
        status_code=exc.status_code,
        detail=exc.error_detail,
        field_path=exc.field_path,
        docs_url=exc.docs_url
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return format_error_response(
        request=request,
        code="VALIDATION_FAILED",
        message="Request payload failed schema validation.",
        status_code=422,
        detail=str(exc)
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled server exception", extra={"request_id": getattr(request.state, "request_id", "unknown")})
    return format_error_response(
        request=request,
        code="INTERNAL",
        message=f"An unexpected internal error occurred: {str(exc)}",
        status_code=500
    )


# Mount Routers
app.include_router(config_router.router)
app.include_router(spec_router.router)
app.include_router(workspace_router.router)
app.include_router(storage_router.router)  # /api/storage/* alias for the React frontend
app.include_router(onboard_router.router)
app.include_router(docs_router.router)
app.include_router(debug_router.router)

# Mount Docs Static Site
docs_dir = Path(__file__).parent.parent / "docs_site"
if docs_dir.exists():
    app.mount("/docs", StaticFiles(directory=str(docs_dir), html=True), name="docs")


def get_static_dist_dir() -> Path:
    candidates = [
        Path(__file__).parent.parent / "static",
        Path(__file__).parent.parent / "web" / "dist",
        Path(__file__).parent / "static",
        Path("static"),
        Path("web/dist"),
    ]
    for c in candidates:
        if c.exists() and (c / "index.html").exists():
            return c.resolve()
    # Fallback to static directory
    fallback = Path(__file__).parent.parent / "static"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback.resolve()


dist_path = get_static_dist_dir()
if dist_path.exists():
    # Vite emits hashed bundles into dist/assets/ and references them as
    # "/assets/index-<hash>.js". Older hand-rolled builds put styles.css and
    # app.js at the dist root and referenced them the same way. Mount whichever
    # layout is actually on disk so both resolve.
    assets_dir = dist_path / "assets"
    app.mount(
        "/assets",
        StaticFiles(directory=str(assets_dir if assets_dir.is_dir() else dist_path)),
        name="assets",
    )


@app.get("/")
async def serve_root():
    """Serve SPA index.html on root."""
    dist = get_static_dist_dir()
    index_file = dist / "index.html"
    if index_file.exists():
        return FileResponse(str(index_file))
    return JSONResponse(status_code=200, content={"status": APP_RUNNING_MESSAGE})


@app.get("/{full_path:path}")
async def serve_spa(full_path: str):
    """Catch-all for SPA client routing and direct asset fetching."""
    if full_path.startswith("api") or full_path.startswith("docs"):
        return JSONResponse(status_code=404, content={"error": "Not Found"})
    dist = get_static_dist_dir()
    if dist:
        target = dist / full_path
        if target.exists() and target.is_file():
            return FileResponse(str(target))
        index_file = dist / "index.html"
        if index_file.exists():
            return FileResponse(str(index_file))
    return JSONResponse(status_code=200, content={"status": APP_RUNNING_MESSAGE})

