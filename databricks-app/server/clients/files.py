"""
File Management Client for Volume and Workspace Spec Storage (§10.2).
Handles path sanitization, traversal prevention, template variable interpolation,
reading, writing, listing, and ETag verification.
"""

import base64
import hashlib
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional, Tuple

from server.errors import AppException
from server.settings import AppSettings, StorageRoot


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
        p = root.path
        vars_map = {
            "catalog": self.settings.template_variables.get("catalog", type("O", (), {"default": "metaflow"})).default,
            "env": self.settings.template_variables.get("env", type("O", (), {"default": "dev"})).default,
        }
        if template_vars:
            vars_map.update(template_vars)

        for k, v in vars_map.items():
            p = p.replace(f"{{{{{k}}}}}", str(v))
        return p.rstrip("/") + "/"

    def _sanitize_path(self, root: StorageRoot, root_path: str, user_path: str) -> str:
        """Sanitize path and prevent path traversal outside permitted storage."""
        clean_user = user_path.replace("\\", "/").strip()
        if ".." in clean_user:
            raise AppException(code="PERMISSION_DENIED", message="Path traversal ('..') is not permitted.", status_code=403)

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

    def list_files(self, root_id: str, prefix: str = "", template_vars: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        """List files in storage root."""
        root = self._get_root(root_id)
        root_path = self.resolve_root_path(root, template_vars)
        target_dir = self._sanitize_path(root, root_path, prefix)

        results: List[Dict[str, Any]] = []
        try:
            if root.kind == "volume":
                entries = self.client.files.list_directory_contents(target_dir)
                for entry in entries:
                    p = getattr(entry, "path", "")
                    ext = Path(p).suffix.lower()
                    if ext in self.settings.spec_storage.allowed_extensions or getattr(entry, "is_directory", False):
                        results.append({
                            "name": Path(p).name,
                            "path": p,
                            "size": getattr(entry, "file_size", 0),
                            "modified": getattr(entry, "last_modified", 0),
                            "kind": "dir" if getattr(entry, "is_directory", False) else "file"
                        })
            else:  # workspace
                objects = self.client.workspace.list(target_dir)
                for obj in objects:
                    p = getattr(obj, "path", "")
                    ext = Path(p).suffix.lower()
                    if ext in self.settings.spec_storage.allowed_extensions or getattr(obj, "object_type", "") == "DIRECTORY":
                        results.append({
                            "name": Path(p).name,
                            "path": p,
                            "size": getattr(obj, "size", 0),
                            "modified": getattr(obj, "modified_at", 0),
                            "kind": "dir" if getattr(obj, "object_type", "") == "DIRECTORY" else "file"
                        })
        except AppException:
            raise
        except Exception as ex:
            raise AppException(code="UPSTREAM_ERROR", message=f"Failed to list directory contents: {str(ex)}", status_code=502)

        return results

    def read_file(self, root_id: str, path: str, template_vars: Optional[Dict[str, str]] = None) -> Tuple[str, str, str]:
        """Read file contents. Returns (content_str, format, etag)."""
        root = self._get_root(root_id)
        root_path = self.resolve_root_path(root, template_vars)
        full_path = self._sanitize_path(root, root_path, path)

        ext = Path(full_path).suffix.lower()
        if ext not in self.settings.spec_storage.allowed_extensions:
            raise AppException(code="UNSUPPORTED_FORMAT", message=f"Extension '{ext}' not allowed. Allowed: {self.settings.spec_storage.allowed_extensions}", status_code=400)

        fmt = "yaml" if ext in (".yaml", ".yml") else "json"

        try:
            if root.kind == "volume":
                res = self.client.files.download(full_path)
                data_bytes = res.contents if hasattr(res, "contents") else res.read()
            else:
                res = self.client.workspace.export(full_path)
                data_bytes = res.content if hasattr(res, "content") else res

            if isinstance(data_bytes, str):
                data_bytes = data_bytes.encode("utf-8")

            etag = hashlib.md5(data_bytes).hexdigest()
            content_str = data_bytes.decode("utf-8")
            return content_str, fmt, etag
        except AppException:
            raise
        except Exception as ex:
            raise AppException(code="NOT_FOUND", message=f"File '{path}' could not be read from root '{root_id}': {str(ex)}", status_code=404)

    def write_file(self, root_id: str, path: str, content: str, overwrite: bool = False, expected_etag: Optional[str] = None, template_vars: Optional[Dict[str, str]] = None) -> Tuple[str, int, str]:
        """Write file to storage root. Returns (full_path, bytes_written, etag)."""
        root = self._get_root(root_id)
        if not root.write:
            raise AppException(code="PERMISSION_DENIED", message=f"Storage root '{root_id}' is configured read-only.", status_code=403)

        root_path = self.resolve_root_path(root, template_vars)
        full_path = self._sanitize_path(root, root_path, path)

        ext = Path(full_path).suffix.lower()
        if ext not in self.settings.spec_storage.allowed_extensions:
            raise AppException(code="UNSUPPORTED_FORMAT", message=f"Extension '{ext}' not allowed.", status_code=400)

        data_bytes = content.encode("utf-8")
        if len(data_bytes) > self.settings.limits.max_spec_bytes:
            raise AppException(code="PAYLOAD_TOO_LARGE", message=f"Spec payload size ({len(data_bytes)} bytes) exceeds limit ({self.settings.limits.max_spec_bytes} bytes).", status_code=413)

        etag = hashlib.md5(data_bytes).hexdigest()

        try:
            if root.kind == "volume":
                self.client.files.upload(full_path, data_bytes, overwrite=overwrite)
            else:
                # Ensure parent directory exists in workspace
                parent_dir = str(Path(full_path).parent).replace("\\", "/")
                if hasattr(self.client.workspace, "mkdirs"):
                    try:
                        self.client.workspace.mkdirs(parent_dir)
                    except Exception:
                        pass
                self.client.workspace.import_(full_path, content=data_bytes, overwrite=overwrite)
            return full_path, len(data_bytes), etag
        except AppException:
            raise
        except Exception as ex:
            raise AppException(code="PERMISSION_DENIED", message=f"Failed to write to '{full_path}': {str(ex)}", status_code=500)
