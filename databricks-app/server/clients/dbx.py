"""
Databricks WorkspaceClient wrapper & Offline Mock Client (§10.1 & §15).
"""

import os
from typing import Any, Dict, List, Optional
from databricks.sdk import WorkspaceClient


class FakeCurrentUser:
    def me(self):
        class UserInfo:
            user_name = "developer@internal.example.com"
            display_name = "Local Developer"
        return UserInfo()


class FakeFilesAPI:
    def __init__(self):
        self._files: Dict[str, bytes] = {}

    def list_directory_contents(self, directory_path: str):
        class DirEntry:
            def __init__(self, path, is_dir=False, size=1024):
                self.path = path
                self.is_directory = is_dir
                self.file_size = size
                self.last_modified = 1714000000

        entries = []
        norm_dir = directory_path.rstrip("/") + "/"
        for p, b in self._files.items():
            if p.startswith(norm_dir):
                sub = p[len(norm_dir):]
                if "/" not in sub:
                    entries.append(DirEntry(p, is_dir=False, size=len(b)))
        # Return at least a mock file if empty
        if not entries:
            entries.append(DirEntry(norm_dir + "sample_spec.json", is_dir=False, size=2048))
        return entries

    def download(self, file_path: str):
        class DownloadResult:
            def __init__(self, contents: bytes):
                self.contents = contents
        data = self._files.get(file_path, b'{"dataflow_group_id":"dfg_sample"}')
        return DownloadResult(data)

    def upload(self, file_path: str, contents: bytes, overwrite: bool = True):
        self._files[file_path] = contents

    def delete(self, file_path: str):
        self._files.pop(file_path, None)


class FakeWorkspaceAPI:
    def __init__(self):
        self._files: Dict[str, bytes] = {}

    def list(self, path: str):
        class ObjectInfo:
            def __init__(self, p):
                self.path = p
                self.object_type = "FILE"
                self.size = 1024
                self.modified_at = 1714000000
        return [ObjectInfo(path.rstrip("/") + "/sample_spec.json")]

    def export(self, path: str, format: str = "AUTO"):
        class ExportResult:
            def __init__(self, content: bytes):
                self.content = content
        return ExportResult(self._files.get(path, b'{"dataflow_group_id":"dfg_sample"}'))

    def import_(self, path: str, format: str = "AUTO", content: bytes = b"", overwrite: bool = True):
        self._files[path] = content


class FakeJobsAPI:
    def __init__(self):
        self._runs: Dict[str, Any] = {}

    def get(self, job_id: int):
        class JobDetails:
            job_id_val = job_id
            settings = None
        return JobDetails()

    def run_now(self, job_id: int, job_parameters: Optional[Dict[str, str]] = None):
        import uuid
        from types import SimpleNamespace

        # NB: this deliberately does not build the result with a class body.
        # `class RunNowResult: run_id = run_id` raises NameError — assigning the
        # name inside the class body makes it class-local, so the right-hand load
        # never reaches the enclosing function's `run_id`. That bug made every
        # job-mode action fail with UPSTREAM_ERROR under METAFLOW_FAKE_DBX.
        generated_run_id = 100000 + (hash(uuid.uuid4().hex) % 899999)
        return SimpleNamespace(run_id=generated_run_id, number_in_job=1)

    def get_run(self, run_id: int):
        class RunDetails:
            state = None
        class State:
            life_cycle_state = "RUNNING"
            result_state = None
            state_message = "Executing stage"
        rd = RunDetails()
        rd.state = State()
        return rd

    def get_run_output(self, run_id: int):
        class RunOutput:
            logs = "Sample execution log output\nTask completed successfully."
        return RunOutput()

    def cancel_run(self, run_id: int):
        pass


class FakeWorkspaceClient:
    """Mock Databricks WorkspaceClient for zero-workspace development and offline testing."""
    def __init__(self):
        self.current_user = FakeCurrentUser()
        self.files = FakeFilesAPI()
        self.workspace = FakeWorkspaceAPI()
        self.jobs = FakeJobsAPI()


def get_workspace_client(token: Optional[str] = None, host: Optional[str] = None, force_fake: bool = False) -> Any:
    """Construct a WorkspaceClient (obo, sp, or mock)."""
    if force_fake or os.environ.get("METAFLOW_FAKE_DBX") == "1" or not token and not os.environ.get("DATABRICKS_HOST"):
        return FakeWorkspaceClient()

    try:
        if token:
            return WorkspaceClient(host=host, token=token)
        return WorkspaceClient(host=host)
    except Exception:
        # Fallback to Fake client if environment is not configured
        return FakeWorkspaceClient()
