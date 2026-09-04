"""Dispatches a built OTel payload to every configured, enabled telemetry destination.

Two destination types (see ``observability_config.destination_type``):

- ``DATABRICKS_VOLUME`` -- write JSONL (optionally gzip-compressed) straight to a Unity
  Catalog Volume path via native UC permissions. No auth/credentials needed or read.
- ``OTLP_CONSUMER`` -- HTTP POST the OTLP/HTTP JSON export request body, with
  ``BEARER_TOKEN``/``API_KEY``/``BASIC_AUTH`` header auth and exponential-backoff retry on
  ``429``/``5xx``.

**One destination's failure must never block delivery to the others.** :func:`dispatch_all`
catches every exception per-destination, logs it via ``structured_logger.log_flow_event``, and
continues -- callers that want "raise if everything failed" get that from
``ObservabilityDispatchError`` when every enabled destination failed (see this module's use in
the entrypoint notebook).

**This module is the *triggered*-mode delivery path only.** It builds one bounded payload and
pushes it once, which is exactly what a post-update export needs and exactly what a standing
stream cannot use. Continuous-mode destinations (``DestinationConfig.mode == "continuous"``, see
``config_loader.py``) are served instead by
``notebooks/06_observability_streaming/06_event_log_otel_streaming_pipeline.py``, which delivers
through genuine Lakeflow sinks (``dlt.create_sink``) fed by ``@dlt.append_flow`` -- a
micro-batch-driven lifecycle with no single "payload" to dispatch at all. Callers must therefore
narrow with ``config_loader.filter_destinations_by_mode(destinations, "triggered")`` *before*
calling :func:`dispatch_all`; handing it a continuous destination would double-export that
destination, once per engine. A group that configured *only* continuous destinations reaches
:func:`dispatch_all` with an empty list and gets its "No enabled observability_config
destinations resolved" error -- correct, and the reason that message names the mode split.

Credential values in ``auth_config`` are always references, never literal secrets --
``env:<VAR_NAME>`` (an environment variable, e.g. injected via a job-cluster/serverless
environment secret) or ``secret:<scope>:<key>`` (a classic Databricks secret scope, resolved via
``dbutils.secrets.get``). This is a deliberately self-contained convention for this module's own
``observability_config`` schema -- distinct from the rest of this framework's Unity-Catalog
3-level secret dict shape (``crypto/secrets.py``), which doesn't fit a single short config string.
"""

import base64
import gzip
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from flowx.lakeflow_framework.exceptions import (
    ObservabilityConfigError,
    ObservabilityDispatchError,
)
from flowx.lakeflow_framework.observability.config_loader import DestinationConfig
from flowx.lakeflow_framework.observability.structured_logger import log_flow_event

logger = logging.getLogger("flowx.lakeflow_framework.observability.destination_dispatcher")

DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_BACKOFF_MULTIPLIER = 2.0
DEFAULT_TIMEOUT_MS = 5000
DEFAULT_BASE_DELAY_SECONDS = 1.0
MAX_BACKOFF_DELAY_SECONDS = 30.0
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


@dataclass
class DispatchResult:
    destination_id: str
    destination_type: str
    status: str  # SUCCESS | FAILED
    uncompressed_bytes: int = 0
    compressed_bytes: int = 0
    duration_ms: float = 0.0
    attempts: int = 0
    http_status_code: Optional[int] = None
    file_path: Optional[str] = None
    error: Optional[str] = None


def resolve_credential(value: str, secret_resolver: Callable[[str, str], str]) -> str:
    """Resolve an ``env:<VAR_NAME>`` or ``secret:<scope>:<key>`` credential reference.

    Parameters
    ----------
    value:
        The raw string from ``auth_config.credentials``.
    secret_resolver:
        ``(scope, key) -> value`` -- called only for ``secret:`` references (typically
        ``lambda scope, key: dbutils.secrets.get(scope, key)``).

    Raises
    ------
    ObservabilityConfigError
        If ``value`` doesn't match either prefix, or the referenced env var/secret is missing.
    """
    if value.startswith("env:"):
        var_name = value[len("env:") :]
        resolved = os.environ.get(var_name)
        if resolved is None:
            raise ObservabilityConfigError(f"Environment variable '{var_name}' referenced by 'env:{var_name}' is not set.")
        return resolved

    if value.startswith("secret:"):
        parts = value.split(":", 2)
        if len(parts) != 3:
            raise ObservabilityConfigError(f"Malformed secret reference {value!r}, expected 'secret:<scope>:<key>'.")
        _, scope, key = parts
        try:
            return secret_resolver(scope, key)
        except Exception as exc:  # noqa: BLE001
            raise ObservabilityConfigError(f"Failed to resolve secret '{scope}/{key}': {exc}") from exc

    raise ObservabilityConfigError(
        f"Credential value {value!r} must be 'env:<VAR_NAME>' or 'secret:<scope>:<key>' -- literal secrets are not allowed."
    )


def build_auth_headers(auth_config: Dict[str, Any], secret_resolver: Callable[[str, str], str]) -> Dict[str, str]:
    """Build HTTP headers for ``auth_config`` (``{}``/``NONE`` -> no headers)."""
    auth_type = (auth_config or {}).get("type", "NONE")
    credentials = (auth_config or {}).get("credentials") or {}

    if auth_type in (None, "NONE"):
        return {}
    if auth_type == "BEARER_TOKEN":
        token = resolve_credential(credentials["token"], secret_resolver)
        return {"Authorization": f"Bearer {token}"}
    if auth_type == "API_KEY":
        header_name = credentials["header_name"]
        api_key = resolve_credential(credentials["api_key"], secret_resolver)
        return {header_name: api_key}
    if auth_type == "BASIC_AUTH":
        username = resolve_credential(credentials["username"], secret_resolver)
        password = resolve_credential(credentials["password"], secret_resolver)
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        return {"Authorization": f"Basic {token}"}
    raise ObservabilityConfigError(f"Unknown auth_config.type {auth_type!r} -- expected BEARER_TOKEN, API_KEY, BASIC_AUTH, or NONE.")


def compress_payload(data: bytes, compression: Optional[str]) -> "tuple[bytes, Optional[str]]":
    """Return ``(payload_bytes, content_encoding_header)``. ``compression`` of ``""``/``None``/
    ``"none"`` (case-insensitive) means send uncompressed -- ``content_encoding_header`` is
    ``None`` in that case."""
    normalized = (compression or "").strip().lower()
    if normalized in ("", "none"):
        return data, None
    if normalized == "gzip":
        return gzip.compress(data), "gzip"
    raise ObservabilityConfigError(f"Unsupported compression {compression!r} -- expected 'gzip' or 'none'/''.")


def compute_backoff_delay_seconds(attempt: int, retry_config: Dict[str, Any], retry_after_header: Optional[str] = None) -> float:
    """Delay before the *next* attempt (``attempt`` is the 1-indexed attempt number that just
    failed). Honors a ``Retry-After`` header (seconds) when present, else exponential backoff
    from ``retry_config.backoff_multiplier`` (default 2.0), capped at
    :data:`MAX_BACKOFF_DELAY_SECONDS`."""
    if retry_after_header:
        try:
            return min(float(retry_after_header), MAX_BACKOFF_DELAY_SECONDS)
        except ValueError:
            pass
    multiplier = retry_config.get("backoff_multiplier", DEFAULT_BACKOFF_MULTIPLIER)
    delay = DEFAULT_BASE_DELAY_SECONDS * (multiplier ** (attempt - 1))
    return min(delay, MAX_BACKOFF_DELAY_SECONDS)


def merge_resource_attributes(resource_logs: List[Dict[str, Any]], extra_attributes: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return a deep-enough copy of ``resource_logs`` with ``extra_attributes`` merged into
    every entry's ``resource.attributes`` -- an existing key of the same name is overridden
    (destination-specific config wins over the canonical framework-built value)."""
    if not extra_attributes:
        return resource_logs

    from flowx.lakeflow_framework.observability.otel_payload_builder import (
        build_attribute,  # local: avoid a cycle at import time
    )

    merged = []
    for entry in resource_logs:
        new_attrs = [attr for attr in entry["resource"]["attributes"] if attr["key"] not in extra_attributes]
        new_attrs.extend(build_attribute(k, v) for k, v in extra_attributes.items())
        merged.append({**entry, "resource": {"attributes": new_attrs}})
    return merged


def _volume_file_path(volume_path: str, dataflow_group_id: str, task_run_id: str, file_format: str, compressed: bool) -> str:
    """The file name itself is deterministic -- `<dataflow_group_id>_<task_run_id>.<ext>` --
    rather than a random UUID, so a file can be identified from its name alone (which run's
    telemetry it holds) without opening it or relying on the directory structure. One
    observability task run dispatches at most one file per DATABRICKS_VOLUME destination, so
    `(dataflow_group_id, task_run_id)` is already collision-free; a re-run with the same
    task_run_id (there isn't one -- Databricks never reuses a run_id) would simply overwrite,
    which is the desired idempotent behavior anyway."""
    date_partition = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    extension = "jsonl" if file_format.upper() == "JSONL" else "json"
    if compressed:
        extension += ".gz"
    base = volume_path.rstrip("/")
    return f"{base}/{dataflow_group_id}/{date_partition}/{dataflow_group_id}_{task_run_id}.{extension}"


def dispatch_to_volume(resource_logs: List[Dict[str, Any]], destination: DestinationConfig, dataflow_group_id: str, task_run_id: str) -> DispatchResult:
    """Write one JSONL line per ``ResourceLogs`` entry to the configured Volume path."""
    config = destination.destination_config
    volume_path = config.get("volume_path")
    if not volume_path:
        return DispatchResult(
            destination_id=destination.destination_id,
            destination_type=destination.destination_type,
            status="FAILED",
            error="destination_config.volume_path is required for DATABRICKS_VOLUME destinations.",
        )

    file_format = config.get("file_format", "JSONL")
    compression = config.get("compression", "none")

    body = "\n".join(json.dumps({"resourceLogs": [entry]}, default=str) for entry in resource_logs).encode("utf-8")
    uncompressed_size = len(body)
    compressed_body, _ = compress_payload(body, compression)

    file_path = _volume_file_path(volume_path, dataflow_group_id, task_run_id, file_format, compressed=(compressed_body is not body))

    start = time.monotonic()
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as f:
            f.write(compressed_body)
        duration_ms = (time.monotonic() - start) * 1000.0
        logger.info(
            "Wrote %d ResourceLogs entr(y/ies) to '%s' (%d -> %d bytes) in %.1f ms",
            len(resource_logs), file_path, uncompressed_size, len(compressed_body), duration_ms,
        )
        return DispatchResult(
            destination_id=destination.destination_id,
            destination_type=destination.destination_type,
            status="SUCCESS",
            uncompressed_bytes=uncompressed_size,
            compressed_bytes=len(compressed_body),
            duration_ms=duration_ms,
            attempts=1,
            file_path=file_path,
        )
    except Exception as exc:  # noqa: BLE001
        duration_ms = (time.monotonic() - start) * 1000.0
        logger.error("Failed to write to Volume path '%s': %s", file_path, exc)
        return DispatchResult(
            destination_id=destination.destination_id,
            destination_type=destination.destination_type,
            status="FAILED",
            uncompressed_bytes=uncompressed_size,
            duration_ms=duration_ms,
            attempts=1,
            file_path=file_path,
            error=str(exc),
        )


def dispatch_to_otlp(
    resource_logs: List[Dict[str, Any]],
    destination: DestinationConfig,
    secret_resolver: Callable[[str, str], str],
    post_fn: Optional[Callable[..., Any]] = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> DispatchResult:
    """POST the OTLP export request body to the configured HTTP endpoint, retrying on
    ``429``/``5xx`` with exponential backoff up to ``retry_config.max_attempts``.

    ``post_fn`` defaults to ``requests.post`` (imported lazily so this module stays importable
    without ``requests`` installed in a context that never dispatches OTLP); tests inject a
    fake to assert retry behavior without real HTTP calls.
    """
    if post_fn is None:
        import requests

        post_fn = requests.post

    config = destination.destination_config
    endpoint = config.get("endpoint")
    if not endpoint:
        return DispatchResult(
            destination_id=destination.destination_id, destination_type=destination.destination_type,
            status="FAILED", error="destination_config.endpoint is required for OTLP_CONSUMER destinations.",
        )

    body = json.dumps({"resourceLogs": resource_logs}, default=str).encode("utf-8")
    uncompressed_size = len(body)
    compressed_body, content_encoding = compress_payload(body, config.get("compression", "none"))

    headers = {"Content-Type": "application/json"}
    if content_encoding:
        headers["Content-Encoding"] = content_encoding
    try:
        headers.update(build_auth_headers(destination.auth_config, secret_resolver))
    except ObservabilityConfigError as exc:
        return DispatchResult(
            destination_id=destination.destination_id, destination_type=destination.destination_type,
            status="FAILED", uncompressed_bytes=uncompressed_size, error=str(exc),
        )

    retry_config = destination.retry_config or {}
    max_attempts = retry_config.get("max_attempts", DEFAULT_MAX_ATTEMPTS)
    timeout_seconds = retry_config.get("timeout_ms", DEFAULT_TIMEOUT_MS) / 1000.0

    start = time.monotonic()
    last_status_code = None
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = post_fn(endpoint, data=compressed_body, headers=headers, timeout=timeout_seconds)
            last_status_code = response.status_code
            if 200 <= response.status_code < 300:
                duration_ms = (time.monotonic() - start) * 1000.0
                logger.info(
                    "POST %s -> %d in %.1f ms (attempt %d/%d, %d -> %d bytes)",
                    endpoint, response.status_code, duration_ms, attempt, max_attempts, uncompressed_size, len(compressed_body),
                )
                return DispatchResult(
                    destination_id=destination.destination_id, destination_type=destination.destination_type,
                    status="SUCCESS", uncompressed_bytes=uncompressed_size, compressed_bytes=len(compressed_body),
                    duration_ms=duration_ms, attempts=attempt, http_status_code=response.status_code,
                )

            last_error = f"HTTP {response.status_code}: {response.text[:500]}"
            if response.status_code not in _RETRYABLE_STATUS_CODES or attempt == max_attempts:
                break
            delay = compute_backoff_delay_seconds(attempt, retry_config, response.headers.get("Retry-After"))
            logger.warning(
                "POST %s -> %d (attempt %d/%d), retrying in %.1fs", endpoint, response.status_code, attempt, max_attempts, delay
            )
            sleep_fn(delay)
        except Exception as exc:  # noqa: BLE001 -- network/timeout errors are retried the same as 5xx
            last_error = str(exc)
            if attempt == max_attempts:
                break
            delay = compute_backoff_delay_seconds(attempt, retry_config)
            logger.warning("POST %s failed (attempt %d/%d): %s -- retrying in %.1fs", endpoint, attempt, max_attempts, exc, delay)
            sleep_fn(delay)

    duration_ms = (time.monotonic() - start) * 1000.0
    logger.error("POST %s failed after %d attempt(s): %s", endpoint, max_attempts, last_error)
    return DispatchResult(
        destination_id=destination.destination_id, destination_type=destination.destination_type,
        status="FAILED", uncompressed_bytes=uncompressed_size, compressed_bytes=len(compressed_body),
        duration_ms=duration_ms, attempts=max_attempts, http_status_code=last_status_code, error=last_error,
    )


def dispatch_all(
    resource_logs: List[Dict[str, Any]],
    destinations: List[DestinationConfig],
    dataflow_group_id: str,
    task_run_id: str,
    secret_resolver: Callable[[str, str], str],
) -> List[DispatchResult]:
    """Dispatch to every destination in ``destinations``, one at a time, isolating failures.

    Raises
    ------
    ObservabilityDispatchError
        If ``destinations`` is empty, or every dispatch attempt failed.
    """
    if not destinations:
        # The leading "No enabled observability_config destinations resolved" phrasing is
        # load-bearing -- agent_tools.py's failure matrix pattern-matches on it, as does
        # docs/25's Error Handling Matrix. Only the trailing explanation was extended in v1.3.0
        # to name the mode split, since an empty list here now has a second possible cause.
        raise ObservabilityDispatchError(
            f"No enabled observability_config destinations resolved for dataflow_group_id='{dataflow_group_id}' -- "
            "either no row is enabled for this group (nor for the '*' global fallback), or every enabled row's "
            "mode excludes this engine (see config_loader.filter_destinations_by_mode)."
        )

    results: List[DispatchResult] = []
    for destination in destinations:
        destination_resource_logs = merge_resource_attributes(
            resource_logs, (destination.destination_config or {}).get("resource_attributes") or {}
        )
        start = time.monotonic()
        try:
            if destination.destination_type == "DATABRICKS_VOLUME":
                result = dispatch_to_volume(destination_resource_logs, destination, dataflow_group_id, task_run_id)
            elif destination.destination_type == "OTLP_CONSUMER":
                result = dispatch_to_otlp(destination_resource_logs, destination, secret_resolver)
            else:
                result = DispatchResult(
                    destination_id=destination.destination_id, destination_type=destination.destination_type,
                    status="FAILED", error=f"Unknown destination_type {destination.destination_type!r}",
                )
        except Exception as exc:  # noqa: BLE001 -- one destination's crash must not stop the others
            result = DispatchResult(
                destination_id=destination.destination_id, destination_type=destination.destination_type,
                status="FAILED", duration_ms=(time.monotonic() - start) * 1000.0, error=str(exc),
            )

        log_flow_event(
            operation="observability_dispatch",
            flow_id=dataflow_group_id,
            status=result.status,
            duration_ms=result.duration_ms,
            error=result.error,
            destination_id=result.destination_id,
            destination_type=result.destination_type,
            uncompressed_bytes=result.uncompressed_bytes,
            compressed_bytes=result.compressed_bytes,
            attempts=result.attempts,
            http_status_code=result.http_status_code,
            file_path=result.file_path,
        )
        results.append(result)

    if all(r.status == "FAILED" for r in results):
        raise ObservabilityDispatchError(
            f"All {len(results)} destination(s) failed for dataflow_group_id='{dataflow_group_id}': "
            f"{[(r.destination_id, r.error) for r in results]}"
        )
    return results
