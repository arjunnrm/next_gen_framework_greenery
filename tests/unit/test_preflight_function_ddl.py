"""Unit tests for control_plane/ddl_definitions.py::get_preflight_function_ddl.

Covers the DDL-string-building contract only (no live Spark/Unity Catalog execution --
consistent with this module's own "pure string-building, no ``spark.sql`` execution here"
design; see ddl_definitions.py's module docstring). The generated Python function *body*'s
runtime behavior (JSON Schema validation against a parsed spec) is exercised indirectly by
shape/round-trip assertions here and directly by test_spec_validator.py's coverage of the
equivalent business rules in spec_validator.py -- the UC function is a structural-only,
JSON-Schema-driven sibling, not a reimplementation, of that validator (see this function's
own docstring).
"""

import base64
import json
from pathlib import Path

from NextGen_Metadata_Framework.lakeflow_framework.control_plane.ddl_definitions import (
    get_preflight_function_ddl,
)

SCHEMA_FILE_PATH = Path(__file__).resolve().parents[2] / "onboarding_templates" / "onboarding_spec.schema.json"


def test_real_schema_file_is_valid_json():
    """The deployed function body does json.loads() on the decoded schema text -- confirm the
    real, committed file is valid JSON (a malformed schema would deploy fine syntactically but
    fail every single live call)."""
    schema_text = SCHEMA_FILE_PATH.read_text(encoding="utf-8")
    json.loads(schema_text)


def test_ddl_shape():
    schema_text = SCHEMA_FILE_PATH.read_text(encoding="utf-8")
    ddl = get_preflight_function_ddl("metaflow.config", schema_text)

    assert ddl.startswith("CREATE OR REPLACE FUNCTION metaflow.config.preflight_check_onboarding_spec(")
    assert "RETURNS STRING" in ddl
    assert "LANGUAGE PYTHON" in ddl
    assert "ENVIRONMENT (dependencies = '[\"jsonschema==4.23.0\", \"pyyaml==6.0.2\"]', environment_version = 'None')" in ddl
    assert ddl.count("AS $$") == 1
    assert ddl.rstrip().endswith("$$")
    # sandboxed body never imports the framework package or pyspark -- structural-only by design
    assert "import NextGen_Metadata_Framework" not in ddl
    assert "import pyspark" not in ddl


def test_schema_text_is_embedded_as_base64_and_round_trips():
    """The schema text is embedded base64-encoded (not as a literal multi-line string), which
    is what makes this immune to backslash/quote/line-count issues in the schema's own content
    -- confirm the embedded value actually decodes back to the exact original bytes."""
    schema_text = SCHEMA_FILE_PATH.read_text(encoding="utf-8")
    ddl = get_preflight_function_ddl("metaflow.config", schema_text)

    marker = '_SCHEMA_B64 = "'
    start = ddl.index(marker) + len(marker)
    end = ddl.index('"', start)
    embedded_b64 = ddl[start:end]

    assert "\n" not in embedded_b64  # single line -- no raw-string/line-count fragility
    decoded = base64.b64decode(embedded_b64).decode("utf-8")
    assert decoded == schema_text
    assert json.loads(decoded) == json.loads(schema_text)


def test_different_catalogs_produce_differently_qualified_function_names():
    schema_text = '{"type": "object"}'
    dev_ddl = get_preflight_function_ddl("metaflow.config", schema_text)
    poc_ddl = get_preflight_function_ddl("poc.config", schema_text)
    assert "metaflow.config.preflight_check_onboarding_spec" in dev_ddl
    assert "poc.config.preflight_check_onboarding_spec" in poc_ddl
