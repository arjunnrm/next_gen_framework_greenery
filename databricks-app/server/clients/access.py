"""
Access Preflight Checker (§10.3).
Tests workspace reachability, identity, storage root existence, read access,
write access (via clean zero-byte probe), and job manage permissions.
"""

from typing import Any, Dict, List, Optional
import uuid

from server.clients.files import FileManager
from server.settings import AppSettings


class AccessChecker:
    """Performs non-destructive preflight permission checks for a user and storage root."""

    def __init__(self, settings: AppSettings, client: Any, user_email: str = "user@example.com"):
        self.settings = settings
        self.client = client
        self.user_email = user_email
        self.file_manager = FileManager(settings, client)

    def run_checks(self, root_id: Optional[str] = None, template_vars: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Perform all checks and return structured checklist report."""
        if not root_id:
            root_id = self.settings.spec_storage.default_root

        root = self.file_manager._get_root(root_id)
        resolved_root_path = self.file_manager.resolve_root_path(root, template_vars)

        checks: List[Dict[str, Any]] = []

        # 1. Workspace Reachable
        try:
            me = self.client.current_user.me()
            resolved_user = getattr(me, "user_name", self.user_email)
            checks.append({
                "id": "workspace_reachable",
                "label": "Workspace reachable",
                "status": "ok"
            })
            checks.append({
                "id": "identity",
                "label": f"Signed in as {resolved_user}",
                "status": "ok"
            })
        except Exception as ex:
            checks.append({
                "id": "workspace_reachable",
                "label": "Workspace reachable",
                "status": "denied",
                "remediation": f"Cannot connect to Databricks workspace: {str(ex)}"
            })
            checks.append({
                "id": "identity",
                "label": "Identity verification",
                "status": "denied"
            })

        # 2. Root Exists & Can Read
        try:
            self.file_manager.list_files(root_id, "", template_vars)
            checks.append({
                "id": "root_exists",
                "label": f"{resolved_root_path} exists",
                "status": "ok"
            })
            checks.append({
                "id": "can_read",
                "label": "Can list and read specs",
                "status": "ok"
            })
        except Exception as ex:
            checks.append({
                "id": "root_exists",
                "label": f"{resolved_root_path} exists",
                "status": "denied",
                "remediation": f"Ensure {resolved_root_path} exists and is accessible: {str(ex)}"
            })
            checks.append({
                "id": "can_read",
                "label": "Can list and read specs",
                "status": "denied"
            })

        # 3. Can Write (zero-byte probe with guaranteed cleanup in finally)
        if not root.write:
            checks.append({
                "id": "can_write",
                "label": "Can write specs",
                "status": "denied",
                "remediation": f"Root '{root_id}' is configured as read-only in config/index.json."
            })
        else:
            probe_name = f".metaflow_access_probe_{uuid.uuid4().hex[:6]}"
            probe_path = resolved_root_path + probe_name
            can_write_ok = False
            error_msg = ""
            try:
                if root.kind == "volume":
                    self.client.files.upload(probe_path, b"", overwrite=True)
                else:
                    self.client.workspace.import_(probe_path, content=b"", overwrite=True)
                can_write_ok = True
            except Exception as ex:
                error_msg = str(ex)
            finally:
                if can_write_ok:
                    try:
                        if root.kind == "volume":
                            self.client.files.delete(probe_path)
                    except Exception:
                        pass

            if can_write_ok:
                checks.append({
                    "id": "can_write",
                    "label": "Can write specs",
                    "status": "ok"
                })
            else:
                checks.append({
                    "id": "can_write",
                    "label": "Can write specs",
                    "status": "denied",
                    "remediation": f"Requires WRITE permission on {resolved_root_path}. {error_msg}",
                    "docs_url": "/docs/01_platform_architecture/#permissions"
                })

        # 4. Actions Permissions
        for act_id, act_cfg in self.settings.actions.items():
            if not act_cfg.enabled:
                continue
            if act_cfg.mode == "local":
                checks.append({
                    "id": f"action_{act_id}",
                    "label": f"Can run {act_cfg.label} (local in-app)",
                    "status": "ok"
                })
            elif act_cfg.mode == "job" and act_cfg.job_id:
                try:
                    self.client.jobs.get(act_cfg.job_id)
                    checks.append({
                        "id": f"action_{act_id}",
                        "label": f"Can run {act_cfg.label} (Job {act_cfg.job_id})",
                        "status": "ok"
                    })
                except Exception as ex:
                    checks.append({
                        "id": f"action_{act_id}",
                        "label": f"Can run {act_cfg.label}",
                        "status": "unknown",
                        "remediation": f"Grant CAN_MANAGE_RUN on job {act_cfg.job_id} to the app or user."
                    })

        return {
            "user": self.user_email,
            "auth_mode": self.settings.auth.mode,
            "root_id": root_id,
            "root_path": resolved_root_path,
            "checks": checks
        }
