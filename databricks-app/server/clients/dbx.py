"""
Databricks WorkspaceClient wrapper & Offline Mock Client (§10.1 & §15).

`FakeWorkspaceClient` exists so the app runs, and the suite tests, with no workspace.
That only has value if the fake enforces the *same contracts as the SDK*. The earlier
fake accepted raw bytes for `workspace.import_(content=...)`, returned bytes from
`files.download().contents` and from `workspace.export().content`, and reported
`object_type` as a plain string. Every one of those is wrong against the real SDK, so
storage code written against the fake passed the whole suite and then failed on every
single save and open in a real workspace.

The fake below therefore mirrors the SDK exactly:
  * `files.upload(path, contents, *, overwrite)` — keyword-only overwrite.
  * `files.download(path).contents` — a **BinaryIO**, not bytes.
  * `workspace.import_(path, *, content, format, language, overwrite)` — `content` is
    **base64 text**; bytes raise, exactly as JSON serialization would.
  * `workspace.export(path, *, format).content` — **base64 text**.
  * `workspace.list(path)[i].object_type` — an **ObjectType enum**, not a string.
It also raises the same `databricks.sdk.errors` types on missing/conflicting paths, so
the error mapping in clients/files.py is exercised rather than assumed.
"""

import base64
import io
import logging
import os
from typing import Any, Dict, List, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceAlreadyExists
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language, ObjectType

from server.branding_generated import env_var

# Imported from the leaf branding module, not from server.settings: deps.py imports this
# module, so reaching back into settings here would create an import cycle.
ENV_FAKE_DBX = env_var("FAKE_DBX")

logger = logging.getLogger("flowx_app")


class FakeCurrentUser:
    def me(self):
        class UserInfo:
            user_name = "developer@internal.example.com"
            display_name = "Local Developer"
        return UserInfo()


class FakeFilesAPI:
    """Mirrors databricks.sdk.service.files.FilesAPI for UC Volume paths."""

    def __init__(self, seed: Optional[Dict[str, bytes]] = None):
        self._files: Dict[str, bytes] = dict(seed or {})

    def list_directory_contents(self, directory_path: str):
        class DirEntry:
            def __init__(self, path, is_dir=False, size=1024):
                self.path = path
                self.name = path.rstrip("/").rsplit("/", 1)[-1]
                self.is_directory = is_dir
                self.file_size = size
                self.last_modified = 1714000000

        entries = []
        seen_dirs = set()
        norm_dir = directory_path.rstrip("/") + "/"
        for p, b in self._files.items():
            if not p.startswith(norm_dir):
                continue
            sub = p[len(norm_dir):]
            if "/" in sub:
                child = sub.split("/", 1)[0]
                if child not in seen_dirs:
                    seen_dirs.add(child)
                    entries.append(DirEntry(norm_dir + child, is_dir=True, size=0))
            else:
                entries.append(DirEntry(p, is_dir=False, size=len(b)))
        return entries

    def download(self, file_path: str):
        """Returns a DownloadResponse-alike whose `contents` is a stream, as the SDK does."""
        if file_path not in self._files:
            raise NotFound(f"The file being accessed is not found: {file_path}")

        class DownloadResult:
            def __init__(self, data: bytes):
                self.contents = io.BytesIO(data)
                self.content_length = len(data)
                self.content_type = "application/octet-stream"
                self.last_modified = "Mon, 01 Jan 2024 00:00:00 GMT"

        return DownloadResult(self._files[file_path])

    def upload(self, file_path: str, contents: Any, *, overwrite: Optional[bool] = None):
        if not overwrite and file_path in self._files:
            raise ResourceAlreadyExists(f"The file being created already exists: {file_path}")
        data = contents.read() if hasattr(contents, "read") else contents
        if isinstance(data, str):
            data = data.encode("utf-8")
        self._files[file_path] = data

    def delete(self, file_path: str):
        if file_path not in self._files:
            raise NotFound(f"The file being accessed is not found: {file_path}")
        self._files.pop(file_path, None)


class FakeWorkspaceAPI:
    """Mirrors databricks.sdk.service.workspace.WorkspaceAPI for /Workspace paths."""

    def __init__(self, seed: Optional[Dict[str, bytes]] = None):
        self._files: Dict[str, bytes] = dict(seed or {})
        self._dirs: set = set()

    def list(self, path: str):
        class ObjectInfo:
            def __init__(self, p, is_dir, size):
                self.path = p
                # Enum, exactly like the SDK — a string here hides the comparison bug.
                self.object_type = ObjectType.DIRECTORY if is_dir else ObjectType.FILE
                self.size = size
                self.modified_at = 1714000000
                self.object_id = abs(hash(p)) % 10**9

        norm_dir = path.rstrip("/") + "/"
        out: List[Any] = []
        seen_dirs = set()
        for p, b in self._files.items():
            if not p.startswith(norm_dir):
                continue
            sub = p[len(norm_dir):]
            if "/" in sub:
                child = sub.split("/", 1)[0]
                if child not in seen_dirs:
                    seen_dirs.add(child)
                    out.append(ObjectInfo(norm_dir + child, True, 0))
            else:
                out.append(ObjectInfo(p, False, len(b)))
        for d in self._dirs:
            parent = d.rstrip("/").rsplit("/", 1)[0] + "/"
            child = d.rstrip("/").rsplit("/", 1)[-1]
            if parent == norm_dir and child not in seen_dirs:
                seen_dirs.add(child)
                out.append(ObjectInfo(d.rstrip("/"), True, 0))
        return out

    def export(self, path: str, *, format: Optional[ExportFormat] = None, outputs: Any = None):
        """Returns an ExportResponse-alike whose `content` is base64 text, as the SDK does."""
        if path not in self._files:
            raise NotFound(f"Path ({path}) doesn't exist.")

        class ExportResult:
            def __init__(self, data: bytes):
                self.content = base64.b64encode(data).decode("ascii")
                self.file_type = path.rsplit(".", 1)[-1] if "." in path else ""

        return ExportResult(self._files[path])

    def import_(
        self,
        path: str,
        *,
        content: Optional[str] = None,
        format: Optional[ImportFormat] = None,
        language: Optional[Language] = None,
        overwrite: Optional[bool] = None,
    ):
        if isinstance(content, (bytes, bytearray)):
            # The SDK puts `content` straight into a JSON body. Bytes fail there, so
            # failing here keeps the fake honest instead of quietly accepting them.
            raise TypeError(
                "workspace.import_ content must be a base64-encoded str, not bytes "
                "(Object of type bytes is not JSON serializable)."
            )
        if format in (None, ImportFormat.SOURCE) and language is None:
            raise ValueError(
                "workspace.import_ requires format=ImportFormat.AUTO for a non-notebook "
                "file; the default SOURCE format needs a `language`."
            )
        if not overwrite and path in self._files:
            raise ResourceAlreadyExists(f"Path ({path}) already exists.")
        self._files[path] = base64.b64decode(content or "", validate=True)

    def mkdirs(self, path: str):
        self._dirs.add(path.rstrip("/") + "/")

    def delete(self, path: str, *, recursive: Optional[bool] = None):
        if path in self._files:
            self._files.pop(path, None)
            return
        norm = path.rstrip("/") + "/"
        if norm in self._dirs:
            self._dirs.discard(norm)
            return
        raise NotFound(f"Path ({path}) doesn't exist.")


class FakeJobsAPI:
    def __init__(self):
        self._runs: Dict[str, Any] = {}

    def get(self, job_id: int):
        class JobDetails:
            job_id_val = job_id
            settings = None
        return JobDetails()

    def run_now(
        self,
        job_id: int,
        *,
        job_parameters: Optional[Dict[str, str]] = None,
        notebook_params: Optional[Dict[str, str]] = None,
        python_params: Optional[List[str]] = None,
        python_named_params: Optional[Dict[str, str]] = None,
        jar_params: Optional[List[str]] = None,
        sql_params: Optional[Dict[str, str]] = None,
        dbt_commands: Optional[List[str]] = None,
        idempotency_token: Optional[str] = None,
        **kwargs: Any,
    ):
        import uuid
        from types import SimpleNamespace

        # NB: this deliberately does not build the result with a class body.
        # `class RunNowResult: run_id = run_id` raises NameError — assigning the
        # name inside the class body makes it class-local, so the right-hand load
        # never reaches the enclosing function's `run_id`. That bug made every
        # job-mode action fail with UPSTREAM_ERROR under METAFLOW_FAKE_DBX.
        generated_run_id = 100000 + (hash(uuid.uuid4().hex) % 899999)
        self._runs[str(generated_run_id)] = {
            "run_id": generated_run_id,
            "job_id": job_id,
            "job_parameters": job_parameters or {},
            "notebook_params": notebook_params or {},
            "python_params": python_params or [],
            "python_named_params": python_named_params or {},
            "jar_params": jar_params or [],
            "sql_params": sql_params or {},
            "idempotency_token": idempotency_token,
            "state": "RUNNING",
            "life_cycle": "RUNNING",
            "result": None,
            "logs": "Sample execution log output\nTask completed successfully.",
        }
        return SimpleNamespace(run_id=generated_run_id, number_in_job=1)

    def get_run(self, run_id: int):
        rdata = self._runs.get(str(run_id), {})

        class State:
            def __init__(self, life_cycle: str, result: Optional[str]):
                self.life_cycle_state = life_cycle
                self.result_state = result
                self.state_message = "Executing stage"

        class RunDetails:
            def __init__(self, run_id_val: int, data: Dict[str, Any]):
                self.run_id = run_id_val
                self.job_id = data.get("job_id", 101)
                self.state = State(data.get("life_cycle", "RUNNING"), data.get("result"))

        return RunDetails(run_id, rdata)

    def get_run_output(self, run_id: int):
        rdata = self._runs.get(str(run_id), {})

        class RunOutput:
            logs = rdata.get("logs", "Sample execution log output\nTask completed successfully.")

        return RunOutput()

    def cancel_run(self, run_id: int):
        if str(run_id) in self._runs:
            self._runs[str(run_id)]["life_cycle"] = "TERMINATED"
            self._runs[str(run_id)]["result"] = "CANCELED"


_SAMPLE_SPEC = (
    b'{\n  "dataflow_group_id": "dfg_sample",\n  "ingestion_flows": [],\n'
    b'  "transformation_flows": [],\n  "reconciliation_flows": []\n}\n'
)


class FakeWorkspaceClient:
    """Mock Databricks WorkspaceClient for zero-workspace development and offline testing."""

    def __init__(self):
        self.current_user = FakeCurrentUser()
        # Seeded so a local run can list, open and re-save something real end to end.
        self.files = FakeFilesAPI({
            "/Volumes/flowx/flowx/onboarding_specs/sample_spec.json": _SAMPLE_SPEC,
        })
        self.workspace = FakeWorkspaceAPI({
            "/Workspace/Shared/flowx/specs/sample_spec.json": _SAMPLE_SPEC,
        })
        self.jobs = FakeJobsAPI()


_FAKE_CLIENT: Optional[FakeWorkspaceClient] = None


def _fake_client() -> FakeWorkspaceClient:
    """One fake per process, not one per request.

    `get_dbx_client` is a per-request FastAPI dependency, so a fresh fake meant a file
    written by POST /write was gone by the time GET /read asked for it: offline mode
    could never round-trip a save, and no test could catch a broken save by reading it
    back. A storage backend that forgets between requests does not model storage.
    """
    global _FAKE_CLIENT
    if _FAKE_CLIENT is None:
        _FAKE_CLIENT = FakeWorkspaceClient()
    return _FAKE_CLIENT


def reset_fake_workspace_client() -> None:
    """Drop the process-wide fake so a test can start from the seeded state."""
    global _FAKE_CLIENT
    _FAKE_CLIENT = None


def get_workspace_client(
    token: Optional[str] = None,
    host: Optional[str] = None,
    user: Optional[str] = None,
    force_fake: bool = False
) -> Any:
    """Construct a WorkspaceClient (obo, sp, or mock).

    A construction failure is *not* silently downgraded to the fake. Doing so made every
    save report success while writing into an in-memory dict that died with the request,
    and every open return the built-in sample instead of the user's file — a failure mode
    indistinguishable from working software until someone looks in the Volume and finds
    nothing there.
    """
    if force_fake or os.environ.get(ENV_FAKE_DBX) == "1":
        logger.debug(
            f"Using mock WorkspaceClient ({ENV_FAKE_DBX}=1).",
            extra={"event": "sdk_client_init", "auth_mode": "fake", "user": user or "system"}
        )
        return _fake_client()

    effective_host = host or os.environ.get("DATABRICKS_HOST")
    if effective_host and not effective_host.startswith("http://") and not effective_host.startswith("https://"):
        effective_host = f"https://{effective_host}"

    if not token and not effective_host:
        logger.debug(
            "No token and no DATABRICKS_HOST; using mock WorkspaceClient.",
            extra={"event": "sdk_client_init", "auth_mode": "fake", "user": user or "system"}
        )
        return _fake_client()

    try:
        if token:
            logger.info(
                f"Initializing WorkspaceClient with OBO token for {user or 'user'} against host {effective_host}.",
                extra={"event": "sdk_client_init", "auth_mode": "obo", "host": effective_host, "user": user or "unknown"}
            )
            return WorkspaceClient(
                host=effective_host,
                token=token,
                auth_type="pat",
                client_id="",
                client_secret="",
            )

        logger.info(
            f"Initializing WorkspaceClient with ambient SP credentials against host {effective_host}.",
            extra={"event": "sdk_client_init", "auth_mode": "sp", "host": effective_host, "user": user or "system"}
        )
        return WorkspaceClient(host=effective_host)
    except Exception as ex:
        logger.error(
            f"Failed to initialize WorkspaceClient: {ex}",
            extra={"event": "sdk_client_init_failed", "host": effective_host, "user": user or "unknown", "error": str(ex)}
        )
        raise
