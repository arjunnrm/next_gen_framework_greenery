# Development Log 006: FastAPI Backend, Databricks Client & REST Routers

## Enactment Summary
- **Component**: FastAPI Application, Databricks Clients (Real & Mock), Routers, and API Test Suite
- **Specification Version**: v1.0 (Targeting Metaflow Framework Schema v1.3.0)
- **Status**: Completed (45/45 Tests Passing)

## Details of Enacted Functionality
1. **Application Settings & Environment (`server/settings.py`)**:
   - Pydantic models for `config/index.json`, auth modes (`obo` / `sp`), storage roots, action configurations, docs, features, limits, and dynamic `config_dir`.
2. **Logging & Error Infrastructure (`server/errors.py`, `server/logging_setup.py`, `server/deps.py`)**:
   - Structured JSON logging per request with request ID tracking (`X-Request-Id`).
   - Standard error envelope matching §9.2 (`error: {code, message, detail, field_path, request_id, docs_url}`).
   - Dependency injection for Databricks `WorkspaceClient`, request ID, and forwarded user identities.
3. **Databricks Integration & Offline Mock Client (`server/clients/`)**:
   - `dbx.py`: Zero-workspace offline development support via `FakeWorkspaceClient` (`METAFLOW_FAKE_DBX=1`).
   - `files.py`: Unity Catalog Volume and Workspace spec file management with path traversal checks, sanitization, format filtering, and ETag hashing.
   - `jobs.py`: Action dispatcher for local, job, and pipeline execution modes with stage progression tracking.
   - `access.py`: Non-destructive preflight permission checklist with probe file cleanup.
4. **REST Routers (`server/routers/`)**:
   - `config_router.py`: `/api/health`, `/api/config`, `/api/templates`, `/api/templates/{id}`.
   - `spec_router.py`: `/api/spec/validate`, `/api/spec/render`, `/api/spec/import`, `/api/spec/diff`.
   - `workspace_router.py`: `/api/workspace/access`, `/api/workspace/list`, `/api/workspace/read`, `/api/workspace/write`.
   - `onboard_router.py`: `/api/actions/{action_id}/run`, `/api/actions/runs/{run_id}`, `/api/actions/runs/{run_id}/cancel`.
   - `docs_router.py`: `/api/docs/resolve`.
   - `debug_router.py`: `/api/debug/config`, `/api/debug/predicate`.
5. **Static Mounts (`server/app.py`, `docs_site/index.html`)**:
   - Mounted embedded documentation site at `/docs/`.
   - Configured single-page app static hosting from `web/dist/`.
6. **API Verification Tests**:
   - `tests/test_api_contract.py`: Endpoint health, config delivery, spec validation, rendering, error envelope.
   - `tests/test_access_check.py`: Access preflight checklist, local action triggering and status polling.
   - `tests/test_paths.py`: Path traversal blocking, unsupported extension rejection, doc link resolution.
