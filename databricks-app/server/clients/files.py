"""
File Management Client for Volume and Workspace Spec Storage (§10.2).
Handles path sanitization, traversal prevention, template variable interpolation,
reading, writing, listing, and ETag verification.

Every call here goes through the Databricks SDK, and each of the four APIs used has a
*different* wire contract for the same conceptual "bytes of a file":

  files.upload()      contents must be a **BinaryIO**, not bytes.
  files.download()    DownloadResponse.contents is a **BinaryIO stream**, not bytes.
  workspace.import_() content must be a **base64 string** (bytes are not JSON
                      serializable), and needs format=AUTO — the default SOURCE format
                      rejects a .json/.yaml file because it has no notebook `language`.
  workspace.export()  ExportResponse.content is a **base64 string**, and again the
                      default SOURCE format only works for notebooks.

Getting any one of these wrong fails at runtime only, against a real workspace, which is
why `FakeWorkspaceClient` in clients/dbx.py now reproduces all four contracts exactly.
"""

import base64
import binascii
import hashlib
import io
import logging
import os
from pathlib import PurePosixPath
import time
from typing import Any, Dict, List, Optional, Tuple

from databricks.sdk.errors import (
    BadRequest,
    DatabricksError,
    InvalidParameterValue,
    NotFound,
    PermissionDenied,
    RequestLimitExceeded,
    ResourceAlreadyExists,
    ResourceConflict,
    ResourceDoesNotExist,
    ResourceExhausted,
    TooManyRequests,
    Unauthenticated,
)
from databricks.sdk.service.workspace import ExportFormat, ImportFormat

from server.errors import AppException
from server.settings import AppSettings, StorageRoot

logger = logging.getLogger("metaflow_app")


def resolve_template_placeholders(
    settings: AppSettings, path: str, template_vars: Optional[Dict[str, str]] = None
) -> str:
    """Substitute `{{catalog}}` / `{{env}}` style placeholders in a configured path.

    Shared with config_router so that the paths handed to the browser and the paths the
    storage layer resolves come from one implementation. When they came from two, the
    API served `/Volumes/{{catalog}}/...` to the SPA, which sent it straight back as a
    literal path — and every save and open aimed at a directory named `{{catalog}}`.
    """
    vars_map = {name: var.default for name, var in settings.template_variables.items()}
    vars_map.setdefault("catalog", "metaflow")
    vars_map.setdefault("env", "dev")
    if template_vars:
        vars_map.update({k: v for k, v in template_vars.items() if v not in (None, "")})

    for k, v in vars_map.items():
        path = path.replace(f"{{{{{k}}}}}", str(v))
    return path


def _object_type(obj: Any) -> str:
    """Normalise a workspace object_type to a plain string.

    The SDK returns `ObjectType.DIRECTORY`, an Enum. `ObjectType.DIRECTORY == "DIRECTORY"`
    is **False**, so a direct string comparison silently classifies every directory as a
    file and then drops it (a directory has no allowed extension). Compare on `.value`.
    """
    raw = getattr(obj, "object_type", "")
    return str(getattr(raw, "value", raw) or "")


def _download_bytes(res: Any) -> bytes:
    """Bytes out of a files.download() response, whose `contents` is a stream."""
    raw = getattr(res, "contents", None)
    if raw is None:
        raw = res
    if hasattr(raw, "read"):
        raw = raw.read()
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    return raw or b""


def _export_bytes(res: Any) -> bytes:
    """Bytes out of a workspace.export() response, whose `content` is base64 text.

    The fallback is safe rather than sloppy: the raw text of any spec this app handles is
    JSON or YAML, which contains `{`, `"`, `:` or newlines and therefore can never be
    valid strict base64. A `binascii.Error` here means "this was already decoded", not
    "the payload is corrupt", so the two cases cannot be confused.
    """
    raw = getattr(res, "content", None)
    if raw is None:
        raw = res
    if isinstance(raw, bytes):
        return raw
    if not isinstance(raw, str):
        return b""
    try:
        return base64.b64decode(raw, validate=True)
    except (binascii.Error, ValueError):
        return raw.encode("utf-8")


class FileManager:
    """Manages spec files stored in Unity Catalog Volumes and Workspace directories."""

    def __init__(self, settings: AppSettings, client: Any):
        self.settings = settings
        self.client = client

    def _get_root(self, root_id: str) -> StorageRoot:
        for r in self.settings.spec_storage.roots:
            if r.id == root_id:
                return r
        raise AppException(
            code="NOT_FOUND",
            message=f"Storage root '{root_id}' not found in configuration.",
            status_code=404
        )

    def resolve_root_path(self, root: StorageRoot, template_vars: Optional[Dict[str, str]] = None) -> str:
        """Resolve template placeholders like {{catalog}} and {{env}} in root path."""
        p = resolve_template_placeholders(self.settings, root.path, template_vars)
        return p.rstrip("/") + "/"

    def _sanitize_path(self, root: StorageRoot, root_path: str, user_path: str) -> str:
        """Sanitize path and prevent path traversal outside permitted storage."""
        clean_user = user_path.replace("\\", "/").strip()
        if ".." in clean_user:
            raise AppException(code="PERMISSION_DENIED", message="Path traversal ('..') is not permitted.", status_code=403)

        # An unresolved placeholder must never reach the workspace. It arrives whenever a
        # caller echoes a configured root path back verbatim; writing it creates a literal
        # `{{catalog}}` directory, and reading it always 404s with a misleading message.
        if "{{" in clean_user or "}}" in clean_user:
            raise AppException(
                code="VALIDATION_FAILED",
                message=(
                    f"Path '{clean_user}' still contains an unresolved template placeholder. "
                    "Substitute the template variables (catalog, env) before calling storage."
                ),
                status_code=400,
            )

        # If user passed a full absolute path
        if clean_user.startswith("/"):
            if root.kind == "volume":
                if not clean_user.startswith("/Volumes/"):
                    raise AppException(code="PERMISSION_DENIED", message="Volume path must start with '/Volumes/'.", status_code=403)
                return clean_user
            elif root.kind == "workspace":
                if not clean_user.startswith("/Workspace/"):
                    raise AppException(code="PERMISSION_DENIED", message="Workspace path must start with '/Workspace/'.", status_code=403)
                return clean_user
            return clean_user

        # Otherwise it's a relative path/filename under the root_path
        full_path = root_path.rstrip("/") + "/" + clean_user.lstrip("/")
        return full_path

    def _upstream(self, ex: Exception, action: str, path: str) -> AppException:
        """Map an SDK failure onto the app's error envelope, preserving the real cause."""
        logger.error(
            f"Databricks SDK error during {action} on '{path}': {ex}",
            extra={"event": f"workspace_{action}_error", "path": path, "error": str(ex), "exception_type": type(ex).__name__}
        )
        if isinstance(ex, Unauthenticated):
            return AppException(code="UNAUTHORIZED", message=f"Authentication failed while attempting to {action} '{path}': {ex}", status_code=401)
        if isinstance(ex, PermissionDenied):
            return AppException(code="PERMISSION_DENIED", message=f"Not permitted to {action} '{path}': {ex}", status_code=403)
        if isinstance(ex, (NotFound, ResourceDoesNotExist)):
            return AppException(code="NOT_FOUND", message=f"'{path}' was not found: {ex}", status_code=404)
        if isinstance(ex, (ResourceAlreadyExists, ResourceConflict)):
            return AppException(code="CONFLICT", message=f"'{path}' already exists. Enable overwrite to replace it.", status_code=409)
        if isinstance(ex, (ResourceExhausted, RequestLimitExceeded, TooManyRequests)):
            return AppException(code="RATE_LIMITED", message=f"Databricks rate limit exceeded during {action} on '{path}': {ex}", status_code=429)
        if isinstance(ex, (InvalidParameterValue, BadRequest)):
            return AppException(code="VALIDATION_FAILED", message=f"Invalid parameter for {action} on '{path}': {ex}", status_code=400)
        return AppException(code="UPSTREAM_ERROR", message=f"Failed to {action} '{path}': {ex}", status_code=502)

    def list_files(self, root_id: str, prefix: str = "", template_vars: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        """List files in storage root."""
        start_time = time.time()
        root = self._get_root(root_id)
        root_path = self.resolve_root_path(root, template_vars)
        target_dir = self._sanitize_path(root, root_path, prefix)

        allowed = self.settings.spec_storage.allowed_extensions
        results: List[Dict[str, Any]] = []

        logger.debug(
            f"Listing files in root '{root_id}' at '{target_dir}'.",
            extra={"event": "workspace_list_start", "root_id": root_id, "path": target_dir, "kind": root.kind}
        )

        try:
            if root.kind == "volume":
                for entry in self.client.files.list_directory_contents(target_dir):
                    p = getattr(entry, "path", "")
                    is_dir = bool(getattr(entry, "is_directory", False))
                    if is_dir or PurePosixPath(p).suffix.lower() in allowed:
                        results.append({
                            "name": PurePosixPath(p).name,
                            "path": p,
                            "size": getattr(entry, "file_size", 0) or 0,
                            "modified": getattr(entry, "last_modified", 0) or 0,
                            "kind": "dir" if is_dir else "file"
                        })
            else:  # workspace
                for obj in self.client.workspace.list(target_dir):
                    p = getattr(obj, "path", "")
                    is_dir = _object_type(obj) in ("DIRECTORY", "REPO")
                    if is_dir or PurePosixPath(p).suffix.lower() in allowed:
                        results.append({
                            "name": PurePosixPath(p).name,
                            "path": p,
                            "size": getattr(obj, "size", 0) or 0,
                            "modified": getattr(obj, "modified_at", 0) or 0,
                            "kind": "dir" if is_dir else "file"
                        })
        except AppException:
            raise
        except Exception as ex:
            raise self._upstream(ex, "list", target_dir)

        results.sort(key=lambda e: (e["kind"] != "dir", e["name"].lower()))
        duration_ms = round((time.time() - start_time) * 1000, 2)
        logger.debug(
            f"Found {len(results)} items in '{target_dir}' ({duration_ms}ms).",
            extra={"event": "workspace_list_success", "root_id": root_id, "path": target_dir, "count": len(results), "duration_ms": duration_ms}
        )
        return results

    def read_file(self, root_id: str, path: str, template_vars: Optional[Dict[str, str]] = None) -> Tuple[str, str, str]:
        """Read file contents. Returns (content_str, format, etag)."""
        start_time = time.time()
        root = self._get_root(root_id)
        root_path = self.resolve_root_path(root, template_vars)
        full_path = self._sanitize_path(root, root_path, path)

        ext = PurePosixPath(full_path).suffix.lower()
        if ext not in self.settings.spec_storage.allowed_extensions:
            raise AppException(code="UNSUPPORTED_FORMAT", message=f"Extension '{ext}' not allowed. Allowed: {self.settings.spec_storage.allowed_extensions}", status_code=400)

        fmt = "yaml" if ext in (".yaml", ".yml") else "json"

        logger.debug(
            f"Reading file from root '{root_id}' at '{full_path}'.",
            extra={"event": "workspace_read_start", "root_id": root_id, "path": full_path, "kind": root.kind}
        )

        try:
            if root.kind == "volume":
                data_bytes = _download_bytes(self.client.files.download(full_path))
            else:
                data_bytes = _export_bytes(self.client.workspace.export(full_path, format=ExportFormat.AUTO))
        except AppException:
            raise
        except Exception as ex:
            raise self._upstream(ex, "read", full_path)

        etag = hashlib.md5(data_bytes).hexdigest()
        try:
            content_str = data_bytes.decode("utf-8")
        except UnicodeDecodeError as ex:
            raise AppException(
                code="UNSUPPORTED_FORMAT",
                message=f"'{full_path}' is not UTF-8 text and cannot be parsed as a spec: {ex}",
                status_code=400,
            )

        duration_ms = round((time.time() - start_time) * 1000, 2)
        logger.info(
            f"Successfully read {len(data_bytes)} bytes from '{full_path}' (etag: {etag}) in {duration_ms}ms.",
            extra={
                "event": "workspace_read_success",
                "root_id": root_id,
                "path": full_path,
                "bytes": len(data_bytes),
                "etag": etag,
                "duration_ms": duration_ms,
                "format": fmt
            }
        )
        return content_str, fmt, etag

    def write_file(self, root_id: str, path: str, content: str, overwrite: bool = False, expected_etag: Optional[str] = None, template_vars: Optional[Dict[str, str]] = None) -> Tuple[str, int, str]:
        """Write file to storage root. Returns (full_path, bytes_written, etag)."""
        start_time = time.time()
        root = self._get_root(root_id)
        if not root.write:
            raise AppException(code="PERMISSION_DENIED", message=f"Storage root '{root_id}' is configured read-only.", status_code=403)

        root_path = self.resolve_root_path(root, template_vars)
        full_path = self._sanitize_path(root, root_path, path)

        ext = PurePosixPath(full_path).suffix.lower()
        if ext not in self.settings.spec_storage.allowed_extensions:
            raise AppException(code="UNSUPPORTED_FORMAT", message=f"Extension '{ext}' not allowed.", status_code=400)

        data_bytes = content.encode("utf-8")
        if len(data_bytes) > self.settings.limits.max_spec_bytes:
            raise AppException(code="PAYLOAD_TOO_LARGE", message=f"Spec payload size ({len(data_bytes)} bytes) exceeds limit ({self.settings.limits.max_spec_bytes} bytes).", status_code=413)

        # Optimistic concurrency verification
        if expected_etag:
            try:
                _, _, current_etag = self.read_file(root_id, path, template_vars=template_vars)
            except AppException as ex:
                if ex.status_code != 404:
                    raise
                current_etag = None
            if current_etag and current_etag != expected_etag:
                raise AppException(
                    code="CONFLICT",
                    message=(
                        f"'{full_path}' changed since it was read (etag {current_etag}, expected "
                        f"{expected_etag}). Re-open the file and re-apply your changes."
                    ),
                    status_code=409,
                )

        etag = hashlib.md5(data_bytes).hexdigest()

        logger.debug(
            f"Writing {len(data_bytes)} bytes to '{full_path}' (overwrite={overwrite}).",
            extra={
                "event": "workspace_write_start",
                "root_id": root_id,
                "path": full_path,
                "bytes": len(data_bytes),
                "overwrite": overwrite,
                "expected_etag": expected_etag,
                "kind": root.kind
            }
        )

        try:
            if root.kind == "volume":
                # contents must be a BinaryIO — the SDK streams it as the request body.
                self.client.files.upload(full_path, io.BytesIO(data_bytes), overwrite=overwrite)
            else:
                # import_ does not create the parent directory; mkdirs is idempotent.
                parent_dir = str(PurePosixPath(full_path).parent)
                if hasattr(self.client.workspace, "mkdirs"):
                    try:
                        self.client.workspace.mkdirs(parent_dir)
                    except Exception:
                        pass
                self.client.workspace.import_(
                    full_path,
                    content=base64.b64encode(data_bytes).decode("ascii"),
                    format=ImportFormat.AUTO,
                    overwrite=overwrite,
                )

            duration_ms = round((time.time() - start_time) * 1000, 2)
            logger.info(
                f"Successfully wrote {len(data_bytes)} bytes to '{full_path}' (etag: {etag}) in {duration_ms}ms.",
                extra={
                    "event": "workspace_write_success",
                    "root_id": root_id,
                    "path": full_path,
                    "bytes": len(data_bytes),
                    "etag": etag,
                    "duration_ms": duration_ms,
                    "overwrite": overwrite
                }
            )
            return full_path, len(data_bytes), etag
        except AppException:
            raise
        except Exception as ex:
            raise self._upstream(ex, "write", full_path)

    def delete_file(self, root_id: str, path: str, template_vars: Optional[Dict[str, str]] = None) -> str:
        """Delete a file from a storage root. Used by the write-access probe cleanup."""
        start_time = time.time()
        root = self._get_root(root_id)
        root_path = self.resolve_root_path(root, template_vars)
        full_path = self._sanitize_path(root, root_path, path)

        logger.debug(
            f"Deleting file '{full_path}' in root '{root_id}'.",
            extra={"event": "workspace_delete_start", "root_id": root_id, "path": full_path, "kind": root.kind}
        )

        try:
            if root.kind == "volume":
                self.client.files.delete(full_path)
            else:
                self.client.workspace.delete(full_path)

            duration_ms = round((time.time() - start_time) * 1000, 2)
            logger.info(
                f"Successfully deleted '{full_path}' in {duration_ms}ms.",
                extra={"event": "workspace_delete_success", "root_id": root_id, "path": full_path, "duration_ms": duration_ms}
            )
        except AppException:
            raise
        except Exception as ex:
            raise self._upstream(ex, "delete", full_path)
        return full_path
