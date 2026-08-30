"""
Storage Router — /api/storage/* alias over the workspace storage endpoints.

The React frontend delivered by Claude Design (web/src/api.js) addresses storage
as `/api/storage/{access,list,read,write}`. The original backend exposes the same
four operations under `/api/workspace/*`. Rather than fork the frontend away from
the design source — or rename the established `/api/workspace` surface and break
existing callers — this module re-registers the *same handler functions* under the
second prefix.

Both prefixes therefore share one implementation: there is no duplicated logic and
no second code path to keep in sync. `/api/workspace/*` remains the canonical name.

Endpoints:
  GET  /api/storage/access
  GET  /api/storage/list
  GET  /api/storage/read
  POST /api/storage/write
  POST /api/storage/validate
"""

from fastapi import APIRouter

from server.routers.workspace_router import (
    get_access_report,
    list_workspace_files,
    read_workspace_file,
    validate_storage_file,
    write_workspace_file,
)

router = APIRouter(prefix="/api/storage", tags=["Storage"])

# Re-register the workspace handlers verbatim. FastAPI re-reads each function's
# signature here, so all Depends()/Query()/Body() declarations carry over intact.
router.add_api_route("/access", get_access_report, methods=["GET"])
router.add_api_route("/list", list_workspace_files, methods=["GET"])
router.add_api_route("/read", read_workspace_file, methods=["GET"])
router.add_api_route("/write", write_workspace_file, methods=["POST"])
router.add_api_route("/validate", validate_storage_file, methods=["POST"])
