"""Agent-facing "is this spec safe to onboard?" preflight tool for onboarding specs (v2 schema).

This module exposes exactly one public, top-level function --
:func:`preflight_check_onboarding_spec` -- with a plain ``(str, str) -> str`` signature (JSON
in the return value, never a Python exception out) so it can be called directly from Python,
*and* registered as a genuine Unity Catalog (UC) Python Function that an agent (Databricks
Genie, a Mosaic AI Agent, a Claude/LangChain/OpenAI-style tool-calling loop, ...) can invoke as
a tool. It never mutates any control table -- it is read-only, dry-run "will this onboarding
attempt work" reconnaissance, layered on top of the framework's own existing validation:

1. Parses ``spec_json_or_yaml_text`` as JSON or YAML (reusing
   ``onboarding/spec_loader.py``'s own ``parse_spec_text`` -- the exact same parsing +
   error-shaping code ``load_and_template_spec`` itself calls -- rather than re-implementing
   JSON/YAML dispatch; see :func:`_parse_spec_text_json_or_yaml` for why the dispatch logic
   itself has to differ slightly: there is no file extension to key off of here).
2. Runs the spec through the **existing**, unmodified ``onboarding/spec_validator.py``'s
   ``validate_spec`` -- every structural/type/allowed-value/SQL-syntax rule lives there and
   only there; this module never reimplements or duplicates a single validation rule.
3. For every ``target_catalog``/``target_schema``/``target_table`` referenced by any
   ``ingestion_flows[]``/``transformation_flows[]`` entry, and every Volume path referenced by
   ``source_config.path``/``source_config.schema_location``, checks live Unity Catalog via
   ``databricks.sdk.WorkspaceClient`` whether it already exists. A target/volume not existing
   yet is reported informationally (``WILL_BE_CREATED``) -- onboarding routinely creates new
   catalogs/schemas/tables, so that alone is never an error. The one exception:
   ``transformation_flows[].source_inputs[].table`` entries are a **hard requirement**
   (``MISSING_REQUIRED`` if absent) *unless* that exact table is itself the target of another
   ingestion/transformation flow in this same spec -- transformation flows resolve
   ``source_inputs`` via a polymorphic table lookup at pipeline-run time, so a source table
   that is neither pre-existing nor produced elsewhere in this same onboarding batch will
   fail that lookup for real; one produced by a sibling flow in this spec resolves in-graph
   (same ``dataflow_group_id``/pipeline) and is never a live Unity Catalog object at onboarding
   time, so its absence is completely normal, not an error.
4. Returns one JSON string report: ``{"valid": bool, "validation_errors": [...],
   "existence_checks": [...], "summary": "..."}`` -- see :func:`preflight_check_onboarding_spec`
   for the full shape. Designed so an LLM agent calling this as a tool gets one unambiguous
   answer to "is this spec safe to onboard" without having to interpret raw exceptions or
   Databricks SDK error text itself.

Registering this as a Unity Catalog Python Function
----------------------------------------------------
UC Python Functions execute in an isolated sandbox: the function BODY -- not a ``def`` you
write yourself -- is the literal Python code inside ``AS $$ ... $$``, with each declared SQL
parameter already bound as a plain local variable of that name, and it must end with a
``return`` (this is genuine, current Databricks syntax -- see Databricks' own worked
``convert_temp`` example in "CREATE FUNCTION (SQL and Python)"). That sandbox has **no**
access to this repo's own installed package, a live Spark session, or (depending on your
workspace's serverless network/UDF-environment policy) even outbound network access to the
workspace REST API that ``databricks.sdk.WorkspaceClient`` needs for step 3 above -- so there
are two realistic ways to register/use this tool, not one:

**Option A -- call this function directly from your agent's tool-calling loop (recommended).**
This is the only option that gets you the *complete* tool (steps 1-4 above, including live
Unity Catalog existence checks and Spark-backed ``transformation_sql`` syntax checking),
because it runs this exact code in a normal Python process with this package, Databricks
Connect/a live Spark session, and ``databricks-sdk`` all genuinely available -- no sandbox
limitations. Wire it up as a tool definition like::

    # Claude / OpenAI / LangChain-style tool-calling loop
    from NextGen_Metadata_Framework.lakeflow_framework.onboarding.uc_spec_preflight import (
        preflight_check_onboarding_spec,
    )

    # tool schema handed to the model:
    #   name: "preflight_check_onboarding_spec"
    #   description: "Checks whether an onboarding spec (JSON or YAML text) is safe to
    #                 onboard: structural validation plus live Unity Catalog existence
    #                 checks. Returns a JSON report; the caller should read `valid`."
    #   input_schema: {spec_json_or_yaml_text: string, catalog: string}
    #
    # tool execution handler:
    def run_tool(tool_input: dict) -> str:
        return preflight_check_onboarding_spec(tool_input["spec_json_or_yaml_text"], tool_input["catalog"])

For a Databricks Mosaic AI Agent / ``unitycatalog-ai``, the equivalent is
``DatabricksFunctionClient().create_python_function(func=preflight_check_onboarding_spec,
catalog=..., schema=..., replace=True)`` (package ``unitycatalog-ai[databricks]``) -- it
introspects this function's signature/docstring and deploys a UC function wrapper for you
whose actual execution still runs this real code path (not the restricted SQL sandbox),
which is exactly what step 3's live SDK/Spark calls need. Check the exact API surface for the
version you have installed; that method name may differ across releases.

**Option B -- a genuine hand-written ``CREATE FUNCTION ... LANGUAGE PYTHON`` SQL function.**
Useful when a caller (e.g. a Genie space or SQL notebook) needs to reach this via plain SQL
with no Python host process at all. The real syntax::

    CREATE OR REPLACE FUNCTION <catalog>.<schema>.preflight_check_onboarding_spec(
      spec_json_or_yaml_text STRING COMMENT 'Full onboarding spec, as JSON or YAML text',
      catalog STRING COMMENT 'Target Unity Catalog name (substituted for any {{catalog}} token)'
    )
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT 'Preflight-checks an onboarding spec. Returns JSON: {valid, validation_errors, existence_checks, summary}.'
    AS $$
    import json
    # Only the parts of this module that need nothing beyond the standard library + a YAML
    # parser (JSON/YAML parsing, and spec_validator's pure-Python structural/type/allowed-
    # value checks) are realistic to inline here -- the UC Python sandbox cannot `import`
    # this repo's own package, a live Spark session, or (unless your workspace's UDF
    # environment/network policy explicitly allows it, e.g. via
    # `ENVIRONMENT (dependencies = '["databricks-sdk"]')` -- confirm that feature is enabled
    # for your workspace before relying on it) `databricks.sdk.WorkspaceClient`. Doing the
    # *full* job (step 3's live existence checks, and Spark-backed transformation_sql syntax
    # checking) from inside this sandbox is therefore not guaranteed to work in every
    # workspace -- prefer Option A for the complete tool, and reserve this SQL wrapper for a
    # structural-only subset, or for a workspace you've confirmed supports the dependencies
    # above.
    return json.dumps({"valid": True, "validation_errors": [], "existence_checks": [], "summary": "..."})
    $$;

Invoked from SQL (Genie, a SQL notebook, ...) as::

    SELECT <catalog>.<schema>.preflight_check_onboarding_spec(:spec_text, 'my_catalog');

Once registered, add it to a Genie space's "Functions" list, or to a Mosaic AI Agent's tool
list via ``UCFunctionToolkit(function_names=["<catalog>.<schema>.preflight_check_onboarding_spec"])``
(``databricks_langchain`` / ``unitycatalog-ai``), for the agent to call automatically.
"""

import json
import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from databricks.sdk import WorkspaceClient

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import OnboardingValidationError
from NextGen_Metadata_Framework.lakeflow_framework.onboarding.spec_loader import (
    parse_spec_text,
    substitute_environment_placeholders,
)
from NextGen_Metadata_Framework.lakeflow_framework.onboarding.spec_validator import validate_spec

logger = logging.getLogger("NextGen_Metadata_Framework.lakeflow_framework.onboarding.uc_spec_preflight")

# /Volumes/<catalog>/<schema>/<volume>[/<anything else>] -- the standard Unity Catalog Volume
# FUSE-mount path shape used throughout this repo's onboarding_templates/ and docs.
_VOLUME_PATH_PATTERN = re.compile(r"^/Volumes/([^/]+)/([^/]+)/([^/]+)(?:/.*)?$")

_REQUIRED_SOURCE_TABLE_NOTE = (
    "REQUIRED source_inputs.table of transformation_flow[{flow_id}] -- transformation flows "
    "resolve source_inputs via a polymorphic table lookup at pipeline-run time, so this table "
    "must already exist in Unity Catalog unless another flow in this same spec creates it"
)


def _parse_spec_text_json_or_yaml(text: str) -> Dict[str, Any]:
    """Parse ``text`` as JSON, falling back to YAML.

    Reuses ``spec_loader.parse_spec_text`` -- the same JSON/YAML parsing and error-shaping
    code ``load_and_template_spec`` itself calls -- for the actual parsing, so this function
    only supplies the *dispatch*: ``load_and_template_spec`` picks JSON vs. YAML from a file
    extension, but this tool receives raw text with no file/extension at all. JSON is valid
    YAML, so trying a strict JSON parse first and falling back to YAML on failure handles
    both formats correctly without asking the caller to say which one they used.
    """
    try:
        return parse_spec_text(text, ".json", "<inline spec text>")
    except OnboardingValidationError as json_error:
        try:
            return parse_spec_text(text, ".yaml", "<inline spec text>")
        except OnboardingValidationError as yaml_error:
            raise OnboardingValidationError(
                f"Spec text is neither valid JSON ({json_error}) nor valid YAML ({yaml_error})."
            ) from yaml_error


def _acquire_spark_session() -> Optional[Any]:
    """Best-effort: return a live SparkSession if one is obtainable here, else ``None``.

    Tries, in order: an already-active session (``pyspark.sql.SparkSession.builder
    .getOrCreate()``, which resolves instantly with no new connection on a Databricks
    cluster/notebook/Job/DLT driver), then a fresh Databricks Connect session
    (``databricks.connect.DatabricksSession`` -- the same mechanism this repo's own test
    suite uses, see ``tests/conftest.py``) for a local/dev-agent context with a configured
    CLI profile. Every failure is swallowed rather than raised: ``spec_validator.validate_spec``
    already tolerates ``spark=None`` by design (its own generic ``except Exception`` around
    ``spark.sql(...)`` turns a missing session into one reported validation error per
    ``transformation_sql``/``transform_sql`` field, never a crash -- see that module's
    ``_validate_sql_syntax``), so degrading gracefully here is safe and keeps this tool
    usable even with no Spark available at all.
    """
    try:
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()
    except Exception as exc:  # noqa: BLE001 - any failure just means "try the next option"
        logger.debug("No ambient SparkSession available (%s); trying Databricks Connect.", exc)

    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "No Spark session available in this execution context (%s) -- "
            "transformation_sql/transform_sql SQL-syntax checking will be skipped (reported "
            "as a benign validation_errors entry per spec_validator's own None-tolerant "
            "design, not a crash).",
            exc,
        )
        return None


def _get_workspace_client() -> Optional[WorkspaceClient]:
    """Best-effort: construct a ``WorkspaceClient`` using the ambient auth (env vars, CLI
    profile, notebook context, ...), or ``None`` if that fails entirely."""
    try:
        return WorkspaceClient()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not construct a Unity Catalog WorkspaceClient (%s); existence checks will be skipped.", exc)
        return None


def _split_volume_path(path: str) -> Optional[Tuple[str, str, str]]:
    """Return ``(catalog, schema, volume_name)`` for a ``/Volumes/<catalog>/<schema>/<volume>/...``
    path, or ``None`` if ``path`` doesn't have that shape (e.g. a non-UC-Volume path)."""
    match = _VOLUME_PATH_PATTERN.match(path.strip())
    if not match:
        return None
    return match.group(1), match.group(2), match.group(3)


def _flow_id(flow: Dict[str, Any]) -> str:
    return flow.get("dataflow_id") or flow.get("flow_step_id") or "<unknown>"


def _target_tuple(flow: Any) -> Optional[Tuple[str, str, str]]:
    if not isinstance(flow, dict):
        return None
    catalog_name, schema_name, table_name = flow.get("target_catalog"), flow.get("target_schema"), flow.get("target_table")
    if (
        isinstance(catalog_name, str)
        and catalog_name
        and isinstance(schema_name, str)
        and schema_name
        and isinstance(table_name, str)
        and table_name
    ):
        return catalog_name, schema_name, table_name
    return None


def _run_existence_checks(
    spec: Dict[str, Any], catalog: str, workspace_client: Optional[WorkspaceClient]
) -> Tuple[List[Dict[str, str]], List[str]]:
    """Check live Unity Catalog existence for every catalog/schema/table/volume this spec
    references. Returns ``(existence_checks, notes)`` -- ``notes`` carries infra-level
    context (e.g. "no WorkspaceClient available") that doesn't belong in ``validation_errors``
    (which is reserved for ``spec_validator``'s own findings) or in any one ``existence_checks``
    entry.
    """
    existence_checks: List[Dict[str, str]] = []
    notes: List[str] = []
    seen: Set[Tuple[str, str]] = set()

    def _add(kind: str, path: str, note: str, exists_check) -> None:
        key = (kind, path)
        if key in seen:
            return
        seen.add(key)
        exists = False
        try:
            exists = exists_check()
        except Exception as exc:  # noqa: BLE001 - defensive: a client-side bug here must not abort the whole report
            logger.warning("Unexpected error checking %s '%s': %s", kind, path, exc)
        status = "EXISTS" if exists else "WILL_BE_CREATED"
        existence_checks.append({"kind": kind, "path": path, "status": status, "note": note})

    def _add_required_table(path: str, note: str, exists_check) -> None:
        key = ("table", path)
        if key in seen:
            return
        seen.add(key)
        exists = False
        try:
            exists = exists_check()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Unexpected error checking required table '%s': %s", path, exc)
        status = "EXISTS" if exists else "MISSING_REQUIRED"
        existence_checks.append({"kind": "table", "path": path, "status": status, "note": note})

    if workspace_client is None:
        notes.append(
            "Could not reach Unity Catalog (no authenticated WorkspaceClient available in this "
            "execution context) -- existence checks were skipped entirely; only spec_validator's "
            "structural results are available."
        )
        return existence_checks, notes

    def _catalog_exists(name: str) -> bool:
        try:
            workspace_client.catalogs.get(name)
            return True
        except Exception:  # noqa: BLE001 - NotFound (and any other resolution failure) both mean "doesn't exist yet"
            return False

    def _schema_exists(full_name: str) -> bool:
        try:
            workspace_client.schemas.get(full_name)
            return True
        except Exception:  # noqa: BLE001
            return False

    def _table_exists(full_name: str) -> bool:
        try:
            workspace_client.tables.get(full_name)
            return True
        except Exception:  # noqa: BLE001
            return False

    def _volume_exists(full_name: str) -> bool:
        try:
            workspace_client.volumes.read(full_name)
            return True
        except Exception:  # noqa: BLE001
            return False

    ingestion_flows = spec.get("ingestion_flows")
    transformation_flows = spec.get("transformation_flows")
    ingestion_flows = ingestion_flows if isinstance(ingestion_flows, list) else []
    transformation_flows = transformation_flows if isinstance(transformation_flows, list) else []

    tagged_flows = [("ingestion_flow", flow) for flow in ingestion_flows] + [
        ("transformation_flow", flow) for flow in transformation_flows
    ]

    # Every target this spec itself creates -- an ingestion or transformation flow's own
    # target_catalog/target_schema/target_table. A transformation flow's source_inputs.table
    # that matches one of these is resolved polymorphically within this same onboarding
    # spec's pipeline graph (see module docstring), never a live Unity Catalog lookup, so its
    # absence from Unity Catalog right now is completely normal.
    in_spec_targets: Set[Tuple[str, str, str]] = set()
    for _, flow in tagged_flows:
        target = _target_tuple(flow)
        if target:
            in_spec_targets.add(target)

    if catalog:
        _add("catalog", catalog, "catalog argument passed to preflight_check_onboarding_spec", lambda: _catalog_exists(catalog))

    for flow_kind, flow in tagged_flows:
        if not isinstance(flow, dict):
            continue
        flow_id = _flow_id(flow)

        target = _target_tuple(flow)
        if target:
            target_catalog, target_schema, target_table = target
            schema_full_name = f"{target_catalog}.{target_schema}"
            table_full_name = f"{target_catalog}.{target_schema}.{target_table}"
            _add("catalog", target_catalog, f"target_catalog of {flow_kind}[{flow_id}]", lambda c=target_catalog: _catalog_exists(c))
            _add(
                "schema",
                schema_full_name,
                f"target_schema of {flow_kind}[{flow_id}]",
                lambda n=schema_full_name: _schema_exists(n),
            )
            _add(
                "table",
                table_full_name,
                f"target_table of {flow_kind}[{flow_id}] -- not existing yet is normal, onboarding may create it",
                lambda n=table_full_name: _table_exists(n),
            )

        if flow_kind == "ingestion_flow":
            source_config = flow.get("source_config")
            if isinstance(source_config, dict):
                for field_name in ("path", "schema_location"):
                    raw_path = source_config.get(field_name)
                    if not isinstance(raw_path, str) or not raw_path:
                        continue
                    parsed = _split_volume_path(raw_path)
                    if not parsed:
                        continue  # not a /Volumes/... path (e.g. zerobus has no filesystem path) -- out of scope
                    volume_full_name = ".".join(parsed)
                    _add(
                        "volume",
                        volume_full_name,
                        f"source_config.{field_name} of ingestion_flow[{flow_id}] ('{raw_path}')",
                        lambda n=volume_full_name: _volume_exists(n),
                    )

        if flow_kind == "transformation_flow":
            source_inputs = flow.get("source_inputs")
            if not isinstance(source_inputs, list):
                continue
            for source_input in source_inputs:
                if not isinstance(source_input, dict):
                    continue
                table_ref = source_input.get("table")
                if not isinstance(table_ref, str) or not table_ref:
                    continue
                parts = table_ref.split(".")
                if len(parts) != 3:
                    notes.append(
                        f"transformation_flow[{flow_id}].source_inputs.table: '{table_ref}' is not a "
                        "'catalog.schema.table' three-level name -- skipped its existence check."
                    )
                    continue
                if tuple(parts) in in_spec_targets:
                    _add(
                        "table",
                        table_ref,
                        f"source_inputs.table of transformation_flow[{flow_id}] -- created by another flow "
                        "in this same onboarding spec (in-pipeline-graph dependency, not a live Unity "
                        "Catalog lookup)",
                        lambda n=table_ref: _table_exists(n),
                    )
                    continue
                _add_required_table(
                    table_ref,
                    _REQUIRED_SOURCE_TABLE_NOTE.format(flow_id=flow_id),
                    lambda n=table_ref: _table_exists(n),
                )

    return existence_checks, notes


def _error_report(validation_errors: List[str], summary: str) -> str:
    return json.dumps(
        {"valid": False, "validation_errors": validation_errors, "existence_checks": [], "summary": summary},
        indent=2,
    )


def preflight_check_onboarding_spec(spec_json_or_yaml_text: str, catalog: str) -> str:
    """Check whether an onboarding spec is safe to onboard. Never raises -- always returns a
    JSON string report, even on a malformed input or an internal error, so an agent calling
    this as a tool always gets a parseable answer.

    Parameters
    ----------
    spec_json_or_yaml_text
        The full onboarding spec, as JSON or YAML text (either is auto-detected -- see
        :func:`_parse_spec_text_json_or_yaml`). May still contain an unresolved ``{{catalog}}``
        token (as authored in ``onboarding_templates/``); it is substituted with ``catalog``
        before parsing, the same way ``spec_loader.load_and_template_spec`` templates a spec
        file. Any ``{{env}}`` token is left untouched (this tool has no ``environment``
        argument -- pre-resolve it yourself if your spec uses one).
    catalog
        The target Unity Catalog name this spec would be onboarded into.

    Returns
    -------
    str
        A JSON object string with these keys:

        - ``valid`` (bool): ``True`` only if ``validation_errors`` is empty AND no
          ``existence_checks`` entry has ``status == "MISSING_REQUIRED"``.
        - ``validation_errors`` (list[str]): verbatim output of
          ``spec_validator.validate_spec`` -- every structural/type/allowed-value/SQL-syntax
          problem found, each already a fully-qualified, human-readable message.
        - ``existence_checks`` (list[dict]): one entry per distinct
          ``{"kind": "catalog"|"schema"|"table"|"volume", "path": str,
          "status": "EXISTS"|"WILL_BE_CREATED"|"MISSING_REQUIRED", "note": str}``
          for every catalog/schema/table/volume this spec references (see module docstring
          for exactly which fields are checked and why only
          ``transformation_flows[].source_inputs[].table`` can produce ``MISSING_REQUIRED``).
        - ``summary`` (str): one human/agent-readable paragraph stating the verdict.
    """
    try:
        if not isinstance(spec_json_or_yaml_text, str) or not spec_json_or_yaml_text.strip():
            return _error_report(
                ["spec_json_or_yaml_text: is required but was missing or empty"],
                "No spec text was provided -- nothing to check.",
            )
        catalog = catalog.strip() if isinstance(catalog, str) else ""
        if not catalog:
            return _error_report(
                ["catalog: is required but was missing or empty"],
                "No target catalog was provided -- nothing to check.",
            )

        # Substitute {{catalog}} exactly like spec_loader.load_and_template_spec does; leave
        # any {{env}} token alone (replacing it with itself is a deliberate no-op) since this
        # tool takes no `environment` argument.
        templated_text = substitute_environment_placeholders(spec_json_or_yaml_text, catalog, "{{env}}")

        try:
            spec = _parse_spec_text_json_or_yaml(templated_text)
        except OnboardingValidationError as exc:
            return _error_report(
                [str(exc)],
                "Spec text could not be parsed as JSON or YAML -- fix the syntax error above before onboarding.",
            )

        spark = _acquire_spark_session()
        _, _, _, _, validation_errors = validate_spec(spark, spec)

        workspace_client = _get_workspace_client()
        existence_checks, existence_notes = _run_existence_checks(spec, catalog, workspace_client)

        missing_required = [check for check in existence_checks if check["status"] == "MISSING_REQUIRED"]
        is_valid = not validation_errors and not missing_required

        summary_parts: List[str] = []
        if is_valid:
            summary_parts.append(
                "Spec is structurally valid and every table this spec depends on either already exists or "
                "is created elsewhere in this same spec -- safe to onboard."
            )
        else:
            if validation_errors:
                summary_parts.append(f"{len(validation_errors)} structural validation error(s) found (see validation_errors).")
            if missing_required:
                missing_names = ", ".join(sorted({check["path"] for check in missing_required}))
                summary_parts.append(
                    f"{len(missing_required)} required source table(s) referenced by a transformation flow do "
                    f"not exist and are not created elsewhere in this spec: {missing_names}."
                )
            summary_parts.append("NOT safe to onboard until the above are resolved.")
        summary_parts.extend(existence_notes)

        report = {
            "valid": is_valid,
            "validation_errors": validation_errors,
            "existence_checks": existence_checks,
            "summary": " ".join(summary_parts),
        }
        return json.dumps(report, indent=2)
    except Exception as exc:  # this tool must never raise; always return a structured JSON report
        logger.exception("Unexpected error while preflight-checking onboarding spec")
        return _error_report(
            [f"Internal error while preflight-checking spec: {exc}"],
            "Preflight check could not complete due to an internal error -- see validation_errors.",
        )
