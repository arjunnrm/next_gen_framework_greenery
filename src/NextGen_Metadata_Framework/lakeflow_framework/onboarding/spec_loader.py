"""Onboarding spec loading and ``{{catalog}}`` / ``{{env}}`` environment templating.

Accepts both JSON and YAML onboarding specs (see ``onboarding_templates/`` for a
byte-for-byte-equivalent pair) -- the file extension picks the parser, and both funnel into
the exact same in-memory dict, so everything downstream (``spec_validator``,
``metadata_upsert``) is completely unaware of which format the spec was authored in.
"""

import hashlib
import json
import logging
import os
from typing import Any, Dict, Tuple

import yaml

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import OnboardingValidationError

logger = logging.getLogger("common.onboarding.spec_loader")

_YAML_EXTENSIONS = {".yaml", ".yml"}


def read_raw_spec_text(dbutils: Any, path: str) -> str:
    """Read the raw spec file contents from a UC Volume, Workspace Files, or DBFS path.

    Tries a plain filesystem ``open()`` first (works for UC Volumes and Workspace Files
    mounted via FUSE), falling back to ``dbutils.fs.head`` for paths only reachable through
    the DBFS API.

    Raises
    ------
    OnboardingValidationError
        If the file cannot be read via either method.
    """
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return handle.read()
    except (OSError, IOError) as direct_read_error:
        try:
            return dbutils.fs.head(path, 10 * 1024 * 1024)
        except Exception as fallback_error:  # noqa: BLE001
            raise OnboardingValidationError(
                f"Failed to read spec file '{path}' via both direct open() ({direct_read_error}) "
                f"and dbutils.fs.head() ({fallback_error})"
            ) from fallback_error


def substitute_environment_placeholders(raw_text: str, catalog: str, environment: str) -> str:
    """Substitute ``{{catalog}}`` / ``{{env}}`` placeholders throughout the raw spec text."""
    return raw_text.replace("{{catalog}}", catalog).replace("{{env}}", environment)


def parse_spec_text(templated_text: str, file_extension: str, source_label: str) -> Dict[str, Any]:
    """Parse already-templated spec text as JSON or YAML, chosen by ``file_extension``.

    Factored out of ``load_and_template_spec`` so any other caller needing to parse spec
    text (e.g. ``onboarding/uc_spec_preflight.py``, which has raw text with no file path to
    take an extension from) reuses this exact parsing + error-shaping logic instead of
    duplicating it -- pass a synthetic ``file_extension``/``source_label`` (e.g. ``".json"``,
    ``"<inline spec text>"``) when there is no real file involved.

    Raises
    ------
    OnboardingValidationError
        If ``templated_text`` is not valid JSON/YAML for the given ``file_extension``, or
        parses to something other than a JSON/YAML object.
    """
    try:
        if file_extension in _YAML_EXTENSIONS:
            spec = yaml.safe_load(templated_text)
        else:
            spec = json.loads(templated_text)
    except (json.JSONDecodeError, yaml.YAMLError) as exc:
        format_name = "YAML" if file_extension in _YAML_EXTENSIONS else "JSON"
        raise OnboardingValidationError(
            f"Spec file '{source_label}' is not valid {format_name} after templating: {exc}"
        ) from exc

    if not isinstance(spec, dict):
        raise OnboardingValidationError(
            f"Spec file '{source_label}' parsed to a {type(spec).__name__}, not an object -- check the top-level structure."
        )
    return spec


def load_and_template_spec(dbutils: Any, path: str, catalog: str, environment: str) -> Tuple[Dict[str, Any], str, str]:
    """Read, template, and parse an onboarding spec (JSON or YAML) in one step.

    The format is chosen purely by file extension (``.yaml``/``.yml`` -> YAML, anything
    else -> JSON) -- both parse into the exact same in-memory dict shape, so
    ``validate_spec``/``metadata_upsert`` never need to know or care which format a given
    spec was authored in. See ``onboarding_templates/`` for a worked, byte-for-byte
    equivalent pair.

    Returns
    -------
    tuple
        ``(spec_dict, templated_spec_text, spec_version)`` where ``spec_version`` is a
        short deterministic hash of the templated text, used for perception audit trails.

    Raises
    ------
    OnboardingValidationError
        If the file can't be read, or the templated text is not valid JSON/YAML.
    """
    raw_text = read_raw_spec_text(dbutils, path)
    templated_text = substitute_environment_placeholders(raw_text, catalog, environment)

    file_extension = os.path.splitext(path)[1].lower()
    spec = parse_spec_text(templated_text, file_extension, path)

    spec_version = hashlib.sha256(templated_text.encode("utf-8")).hexdigest()[:16]
    logger.info("Loaded spec '%s' (version=%s) for group '%s'", path, spec_version, spec.get("dataflow_group_id"))
    return spec, templated_text, spec_version
