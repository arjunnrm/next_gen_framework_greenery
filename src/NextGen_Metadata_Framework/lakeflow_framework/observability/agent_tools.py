"""Real Python implementations backing the 3 AI Agent / Copilot tools this module ships
(Deliverable 6 -- see ``agent_skills/dlt_observability_tools.json`` for the declarative
OpenAI-function-calling-style tool specs an agent framework loads, and
``docs/25_dlt_observability_module.md``'s Error Handling Matrix, which
:func:`diagnose_pipeline_telemetry_failures` is a machine-readable mirror of).

All three are pure Python -- no Spark, no live workspace call -- so an agent (or a unit test)
can call them directly against arbitrary text input. This mirrors
``onboarding/uc_spec_preflight.py``'s own "Option A/B" split: a lightweight, dependency-light
tool an agent calls directly, versus a live-Unity-Catalog-checking sibling that would need a
real Spark/workspace session (out of scope for these three, by design -- see each function's
docstring for exactly what it does and does not check).
"""

import json
import logging
import os
import re
from typing import Any, Dict, List, Optional

import yaml
from jsonschema import Draft202012Validator

logger = logging.getLogger("NextGen_Metadata_Framework.lakeflow_framework.observability.agent_tools")

_ONBOARDING_SCHEMA_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..", "onboarding_templates", "onboarding_spec.schema.json"
)


def _load_observability_fragment_schema() -> Dict[str, Any]:
    """Build a standalone schema for just the ``{"observability": [...]}`` fragment, reusing
    ``onboarding_spec.schema.json``'s own ``observability`` property and ``$defs`` verbatim --
    there is no separate observability schema file (the ``observability[]`` array is validated
    as part of the full onboarding spec by ``onboarding/spec_validator.py`` at onboarding time;
    this is the same shape, just wrapped so a fragment can be validated on its own before it's
    merged into a full spec).
    """
    with open(os.path.abspath(_ONBOARDING_SCHEMA_PATH), "r", encoding="utf-8") as f:
        full_schema = json.load(f)
    return {
        "$schema": full_schema.get("$schema"),
        "type": "object",
        "required": ["observability"],
        "properties": {"observability": full_schema["properties"]["observability"]},
        "$defs": full_schema["$defs"],
    }


def validate_observability_config(config_text: str, catalog: str = "", env: str = "") -> Dict[str, Any]:
    """Lint/validate an ``{"observability": [...]}`` JSON or YAML fragment against the same
    ``observability`` schema ``onboarding/spec_validator.py`` enforces at onboarding time
    (via ``onboarding_templates/onboarding_spec.schema.json`` -- there is no separate
    observability config file or schema; this validates the fragment on its own, before it's
    merged into a full onboarding spec's own ``observability[]`` array).

    Parameters
    ----------
    config_text:
        Full document text, JSON or YAML (tried in that order); may contain unresolved
        ``{{catalog}}``/``{{env}}`` placeholders, substituted first if ``catalog``/``env`` are
        given (empty string leaves the placeholder as literal text, which will then usually
        fail the ``volume_path``/``endpoint`` pattern checks -- surfacing the omission as a
        validation error rather than silently onboarding a broken path).
    catalog, env:
        Values substituted for ``{{catalog}}``/``{{env}}`` before parsing, matching
        ``onboarding/spec_loader.py::substitute_environment_placeholders``'s convention.

    Returns
    -------
    dict
        ``{"valid": bool, "errors": [str, ...], "summary": str}`` -- structural-only (JSON
        Schema) validation. Does **not** check that a referenced ``env:``/``secret:``
        credential actually resolves, or that an OTLP endpoint is reachable -- see
        :func:`diagnose_pipeline_telemetry_failures` for post-hoc failure diagnosis instead.
    """
    substituted = config_text.replace("{{catalog}}", catalog).replace("{{env}}", env)

    parsed: Optional[Any] = None
    parse_error: Optional[str] = None
    try:
        parsed = json.loads(substituted)
    except json.JSONDecodeError as json_exc:
        try:
            parsed = yaml.safe_load(substituted)
        except yaml.YAMLError as yaml_exc:
            parse_error = f"neither valid JSON ({json_exc}) nor valid YAML ({yaml_exc})"

    if parse_error is not None or not isinstance(parsed, dict):
        return {
            "valid": False,
            "errors": [f"Config text could not be parsed: {parse_error or 'top-level value is not a JSON/YAML object'}"],
            "summary": "Parse error -- fix the syntax before merging into an onboarding spec.",
        }

    validator = Draft202012Validator(_load_observability_fragment_schema())
    schema_errors = sorted(validator.iter_errors(parsed), key=lambda e: list(e.path))
    errors = [f"{'/'.join(str(p) for p in e.path) or '<root>'}: {e.message}" for e in schema_errors]
    is_valid = not errors

    return {
        "valid": is_valid,
        "errors": errors,
        "summary": "Config is schema-valid." if is_valid else f"{len(errors)} validation error(s) found -- see errors.",
    }


#: Mirrors ``observability/config_loader.py::ALLOWED_DESTINATION_MODES`` and the
#: ``observabilityDestination.mode`` enum in ``onboarding_spec.schema.json``. Duplicated here
#: rather than imported so this module stays the dependency-light, pure-Python tool surface its
#: docstring promises (importing config_loader would drag pyspark in for an agent that only wants
#: to lint a JSON fragment).
_ALLOWED_DESTINATION_MODES = ("triggered", "continuous")


def _mode_key(params: Dict[str, Any]) -> Dict[str, Any]:
    """The ``{"mode": ...}`` fragment for a generated destination, or ``{}`` when unspecified.

    Omitting the key entirely (rather than always emitting ``"triggered"``) is deliberate: the
    schema's own default is ``"triggered"``, so an omitted key and an explicit ``"triggered"``
    mean exactly the same thing to every reader -- and the omitted form additionally validates
    against a pre-v1.3.0 ``onboarding_spec.schema.json``, whose ``observabilityDestination`` is
    ``additionalProperties: false`` and would reject the key outright. A generated fragment
    therefore only carries ``mode`` when the caller genuinely asked for one.
    """
    mode = params.get("mode")
    if mode is None:
        return {}
    if mode not in _ALLOWED_DESTINATION_MODES:
        raise KeyError(f"Unknown destination mode {mode!r} -- expected one of {list(_ALLOWED_DESTINATION_MODES)}")
    return {"mode": mode}


def _continuous_destination_config(params: Dict[str, Any]) -> Dict[str, Any]:
    """The ``{"event_log_tables": [...]}`` fragment a ``mode: "continuous"`` destination requires.

    The continuous engine is always-on and has no upstream task to resolve a pipeline from, so it
    must be told which event-log tables to stream -- which is why ``event_log_tables`` is required
    for (and only meaningful in) continuous mode. Raising a plain ``KeyError`` when it is missing
    matches this module's existing convention for a caller who omitted a template's required
    parameter (see :func:`generate_pipeline_onboarding_config`'s ``Raises`` section): generating a
    fragment the validator is guaranteed to reject would only move the failure later.
    """
    if params.get("mode") != "continuous":
        return {}
    return {"event_log_tables": params["event_log_tables"]}


# Known destination "shapes" generate_pipeline_onboarding_config can fill in from a short target
# name, so an agent/caller doesn't need to know the full destination_config shape up front --
# it only supplies what's genuinely specific to this pipeline (volume path, OTLP endpoint/auth).
_DESTINATION_TEMPLATES = {
    "databricks_volume": lambda params: {
        "id": params.get("destination_id", "dest-volume"),
        "enabled": True,
        "type": "DATABRICKS_VOLUME",
        "destination_config": {
            "volume_path": params["volume_path"],
            "compression": params.get("compression", "GZIP"),
            "file_format": params.get("file_format", "JSONL"),
            **_continuous_destination_config(params),
        },
        **_mode_key(params),
    },
    "otlp_http": lambda params: {
        "id": params.get("destination_id", "dest-otlp"),
        "enabled": True,
        "type": "OTLP_CONSUMER",
        "destination_config": {
            "endpoint": params["endpoint"],
            "protocol": "OTLP_HTTP_JSON",
            "compression": params.get("compression", "gzip"),
            "resource_attributes": params.get("resource_attributes", {}),
            **_continuous_destination_config(params),
        },
        **({"auth": params["auth"]} if params.get("auth") else {}),
        "retry": {"max_attempts": params.get("max_attempts", 3)},
        "timeout_ms": params.get("timeout_ms", 5000),
        **_mode_key(params),
    },
}


def generate_pipeline_onboarding_config(
    dataflow_group_id: str,
    destination_targets: List[Dict[str, Any]],
    service_name: Optional[str] = None,
    deployment_environment: Optional[str] = None,
) -> Dict[str, Any]:
    """Auto-generate an ``observability[]`` array to merge into a pipeline's onboarding spec,
    from a short list of destination target descriptors -- so a caller doesn't have to
    hand-author the full ``destination_config`` shape for common destination types.

    Parameters
    ----------
    dataflow_group_id:
        Informational only (surfaced in the log line below) -- the returned
        ``observability[]`` array carries no scoping of its own; it takes on whatever
        ``dataflow_group_id`` the onboarding spec it gets merged into declares, exactly like
        ``ingestion_flows``/``transformation_flows``/``reconciliation_flows`` already do (see
        ``onboarding/metadata_upsert.py::upsert_observability_config``).
    destination_targets:
        One dict per destination, each with ``"template"`` (one of
        :data:`_DESTINATION_TEMPLATES`'s keys: ``"databricks_volume"``, ``"otlp_http"``) plus
        that template's required parameters (``volume_path`` for ``databricks_volume``;
        ``endpoint`` for ``otlp_http``; both accept an optional ``destination_id``).

        Either template also accepts an optional ``"mode"`` (``"triggered"``/``"continuous"``,
        see :func:`_mode_key`) selecting which of the two observability engines serves the
        destination. ``"continuous"`` additionally **requires** ``"event_log_tables"`` -- the
        fully-qualified ``catalog.schema.event_log_table`` names the always-on streaming
        pipeline should read -- since that engine has no upstream task to resolve a pipeline
        from. Omitting ``"mode"`` generates today's shape exactly, which the schema reads as
        ``"triggered"``.
    service_name, deployment_environment:
        When given, merged into every ``otlp_http`` destination's
        ``destination_config.resource_attributes`` as ``service.name``/``deployment.environment``
        (a per-target ``resource_attributes`` entry still wins on key collision).

    Returns
    -------
    dict
        ``{"observability": [...]}`` -- validate with :func:`validate_observability_config`
        first, then merge its ``"observability"`` array directly into the target pipeline's
        onboarding spec (alongside ``ingestion_flows``/``transformation_flows``) so the next
        ``CREATE``/``UPDATE`` onboarding run upserts it via ``upsert_observability_config``.

    Raises
    ------
    KeyError
        If a destination target names an unknown template or an unknown ``mode``, or omits one
        of that template's required parameters -- including ``event_log_tables`` for a
        ``"continuous"`` destination (surfaced as a plain ``KeyError`` naming the missing field,
        since this is a generation-time caller mistake, not a runtime config error).
    """
    generated = []
    for target in destination_targets:
        template_name = target["template"]
        if template_name not in _DESTINATION_TEMPLATES:
            raise KeyError(f"Unknown destination template {template_name!r} -- expected one of {sorted(_DESTINATION_TEMPLATES)}")
        entry = _DESTINATION_TEMPLATES[template_name](target)
        if template_name == "otlp_http" and (service_name or deployment_environment):
            resource_attributes = dict(entry["destination_config"].get("resource_attributes") or {})
            if service_name:
                resource_attributes.setdefault("service.name", service_name)
            if deployment_environment:
                resource_attributes.setdefault("deployment.environment", deployment_environment)
            entry["destination_config"]["resource_attributes"] = resource_attributes
        generated.append(entry)

    logger.info(
        "Generated %d destination(s) for dataflow_group_id='%s': %s",
        len(generated),
        dataflow_group_id,
        # (id, mode) rather than id alone: two destinations can now differ only by which engine
        # serves them, so the id on its own no longer identifies what was generated.
        [(d["id"], d.get("mode", "triggered")) for d in generated],
    )
    return {"observability": generated}


# (pattern, category, likely_cause, remediation) -- checked in order, first match wins. Mirrors
# docs/25_dlt_observability_module.md's Error Handling Matrix; keep the two in sync when either
# changes (see that doc's own note pointing back here).
_FAILURE_MATRIX: List[Dict[str, str]] = [
    {
        "pattern": r"is not a pipeline_task run",
        "category": "task_wiring",
        "likely_cause": "The observability task's pipeline_task_run_id parameter was pointed at a task that "
        "isn't the pipeline_task -- e.g. a notebook task's run_id was passed instead.",
        "remediation": "In the job's task graph, confirm the observability task's base_parameters reads "
        "'{{tasks.<pipeline_task_key>.run_id}}' where <pipeline_task_key> is literally the task_key of the "
        "pipeline_task (conventionally 'run_pipeline_update'), and that the observability task's depends_on "
        "names that same task_key.",
    },
    {
        "pattern": r"no start_time|no end_time yet",
        "category": "task_wiring",
        "likely_cause": "The observability task ran before run_pipeline_update finished, or was triggered "
        "independently rather than as a depends_on-chained downstream task.",
        "remediation": "Add `depends_on: [{task_key: run_pipeline_update}]` to the observability task in the "
        "job resource YAML so the Jobs scheduler guarantees ordering.",
    },
    {
        "pattern": r"no 'dataflow\.group\.id' configuration entry",
        "category": "pipeline_config",
        "likely_cause": "The target pipeline was not deployed through this framework's engine notebook (or its "
        "'dataflow.group.id' Spark conf was removed), so the observability engine cannot resolve which "
        "dataflow_group_id its telemetry belongs to.",
        "remediation": "Confirm the pipeline resource YAML sets `configuration: {dataflow.group.id: <id>}` (see "
        "any resources/*_pipeline.yml for the pattern) and redeploy.",
    },
    {
        "pattern": r"No enabled observability_config destinations resolved",
        "category": "destination_config",
        "likely_cause": "observability_config has no enabled row for this dataflow_group_id or the '*' global "
        "fallback that the *triggered* engine serves -- either the onboarding spec's 'observability' array was "
        "empty/absent, every matching row has enabled=false, or every enabled row declares mode='continuous' "
        "(those are served only by the always-on streaming pipeline, never by this batch engine).",
        "remediation": "Add an 'observability' array to this dataflow_group_id's onboarding spec (or a spec with "
        "dataflow_group_id='*') and re-run onboarding (CREATE/UPDATE), or flip enabled=true on the relevant "
        "observability_config row(s). If the rows exist but all say mode='continuous', either set mode='triggered' "
        "on the one this post-update task should export, or remove this observability task from the job -- a "
        "continuous destination needs notebooks/06_observability_streaming, not this notebook.",
    },
    {
        "pattern": r"mode must be one of",
        "category": "destination_config",
        "likely_cause": "An observability_config row's 'mode' column holds something other than 'triggered' or "
        "'continuous' -- almost always a hand-edited control-table row, since spec_validator rejects any other "
        "value at onboarding time. NULL/absent is legal (it means 'triggered').",
        "remediation": "UPDATE that observability_config row to mode='triggered' or 'continuous' (or NULL for the "
        "'triggered' default), or re-run onboarding (CREATE/UPDATE) from the spec so metadata_upsert rewrites the "
        "row correctly.",
    },
    {
        "pattern": r"The 'obs_mode' widget must be 'triggered'",
        "category": "task_wiring",
        "likely_cause": "notebooks/08_observability/08_dlt_observability_engine.py was run with obs_mode set to "
        "'continuous'. The two modes are separate notebooks with separate lifecycles, not two settings of one "
        "entrypoint -- this batch engine is bounded and post-update; continuous export needs an always-on pipeline.",
        "remediation": "Set the observability task's obs_mode widget back to 'triggered'. To export continuously, "
        "deploy and run resources/observability/observability_otel_streaming_pipeline.yml "
        "(notebooks/06_observability_streaming/06_event_log_otel_streaming_pipeline.py) instead, and mark the "
        "destination mode='continuous' in the onboarding spec.",
    },
    {
        "pattern": r"No continuous observability destinations found in observability_config",
        "category": "destination_config",
        "likely_cause": "The continuous streaming pipeline found neither an observability_config row with "
        "mode='continuous' for its configured dataflow.group.id (it may also simply have no dataflow.group.id / "
        "dataflow.control.catalog configuration, so it never looked) nor a "
        "dataflow.otel_streaming.event_log_tables fallback -- so it has no event-log tables to stream.",
        "remediation": "Either onboard a destination with mode='continuous' and "
        "destination_config.event_log_tables (and set dataflow.group.id + dataflow.control.catalog in "
        "resources/observability/observability_otel_streaming_pipeline.yml's configuration: block so the pipeline can read "
        "observability_config at all), or set the dataflow.otel_streaming.event_log_tables configuration key to a "
        "JSON array of fully-qualified event-log table names.",
    },
    {
        "pattern": r"Environment variable '.*' referenced by 'env:.*' is not set",
        "category": "credentials",
        "likely_cause": "An auth_config credential reference points at an environment variable that isn't "
        "injected into this task's cluster/serverless environment.",
        "remediation": "Add the named environment variable as a secret-backed environment variable on the job "
        "cluster/serverless environment (never a literal secret value in observability_config).",
    },
    {
        "pattern": r"Malformed secret reference|Failed to resolve secret",
        "category": "credentials",
        "likely_cause": "A 'secret:<scope>:<key>' auth_config reference is malformed, or the named classic "
        "Databricks secret scope/key doesn't exist or isn't readable by the job's identity.",
        "remediation": "Verify the secret scope/key with `databricks secrets list-secrets <scope>`, and that the "
        "job's run-as identity has READ on that scope.",
    },
    {
        "pattern": r"volume_path is required",
        "category": "destination_config",
        "likely_cause": "A DATABRICKS_VOLUME destination_config is missing volume_path.",
        "remediation": "Add destination_config.volume_path (must start with /Volumes/) to that destination's row.",
    },
    {
        "pattern": r"endpoint is required",
        "category": "destination_config",
        "likely_cause": "An OTLP_CONSUMER destination_config is missing endpoint.",
        "remediation": "Add destination_config.endpoint (a full https:// URL) to that destination's row.",
    },
    {
        "pattern": r"HTTP 429",
        "category": "destination_outage",
        "likely_cause": "The OTLP destination is rate-limiting this pipeline's telemetry volume even after "
        "exhausting retry_config.max_attempts.",
        "remediation": "Raise retry_config.max_attempts/backoff_multiplier, or reduce telemetry volume (fewer "
        "flows per dataflow_group_id, or a less frequent pipeline schedule); check the destination's own "
        "rate-limit dashboard.",
    },
    {
        "pattern": r"HTTP 5\d\d",
        "category": "destination_outage",
        "likely_cause": "The OTLP destination itself returned a server error on every retry attempt -- likely a "
        "genuine outage or misconfiguration on the receiving side, not this pipeline.",
        "remediation": "Check the destination service's own status page; if it's a self-hosted OTel Collector, "
        "check its own logs for the corresponding rejected request.",
    },
    {
        "pattern": r"Unsupported compression",
        "category": "destination_config",
        "likely_cause": "destination_config.compression is set to a value other than 'gzip'/'GZIP'/'none'/''.",
        "remediation": "Fix the compression value to one of the allowed values (see "
        "docs/27_dlt_observability_onboarding_reference.md's attribute dictionary).",
    },
    {
        "pattern": r"literal secrets are not allowed",
        "category": "credentials",
        "likely_cause": "auth_config.credentials contains a literal token/key/password string instead of an "
        "'env:<VAR_NAME>' or 'secret:<scope>:<key>' reference.",
        "remediation": "Replace the literal value with an env:/secret: reference and store the real secret in an "
        "environment variable or Databricks secret scope instead.",
    },
    {
        "pattern": r"Failed to query event_log",
        "category": "event_log_access",
        "likely_cause": "The job's run-as identity lacks CAN_VIEW/CAN_MANAGE on the pipeline, or the pipeline has "
        "never run an update (event_log() has nothing to return from).",
        "remediation": "Grant the job's run-as identity pipeline permissions, and confirm the pipeline has at "
        "least one completed update before expecting telemetry.",
    },
]


def diagnose_pipeline_telemetry_failures(error_message: str) -> Dict[str, Any]:
    """Match a failed observability task run's error message against the known failure matrix
    and return a diagnosis + remediation steps.

    Parameters
    ----------
    error_message:
        The exception message/traceback text from a failed observability task run (e.g. copied
        from the Jobs UI "Error" panel, or read programmatically from the Jobs API run output).

    Returns
    -------
    dict
        ``{"matched": bool, "category": str | None, "likely_cause": str | None,
        "remediation": str | None}`` -- ``matched=False`` when no known pattern hits, in which
        case the caller should fall back to reading ``docs/25_dlt_observability_module.md``'s
        Error Handling Matrix / troubleshooting runbook directly, or open a genuinely new
        failure mode for that doc's maintainers to add.
    """
    for entry in _FAILURE_MATRIX:
        if re.search(entry["pattern"], error_message):
            return {
                "matched": True,
                "category": entry["category"],
                "likely_cause": entry["likely_cause"],
                "remediation": entry["remediation"],
            }
    return {"matched": False, "category": None, "likely_cause": None, "remediation": None}
