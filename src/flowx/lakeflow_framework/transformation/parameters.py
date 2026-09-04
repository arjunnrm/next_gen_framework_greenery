"""Dynamic runtime parameter substitution for ``${param}`` placeholders.

Resolves ``${param}`` placeholders against a dataflow group's ``pipeline_parameters``
(injected via the onboarding spec, see ``control_plane/ddl_definitions.py``'s
``pipeline_parameters_json`` column) -- re-read fresh on every pipeline update, unlike the
``{{catalog}}``/``{{env}}`` placeholders ``onboarding/spec_loader.py`` resolves once, forever,
at onboarding time. Two renderers exist, matched to two different kinds of text:

* :func:`substitute_dynamic_parameters` -- for **SQL expression text** (a transformation
  flow's ``transformation_sql``, or a reconciliation flow's ``transform_sql``/
  ``filter_condition``), applied right before that text reaches ``spark.sql(...)``/
  ``df.filter(...)``. Every value is rendered as a SQL literal: strings are single-quoted
  (with embedded quotes doubled/escaped), everything else uses its plain Python ``str()``.
  This is exactly why a placeholder must never be wrapped in the author's own quotes in
  ``transformation_sql`` -- ``WHERE country = '${filter_country}'`` becomes ``WHERE country =
  ''US''`` (a ``ParseException``) for a string parameter, because this function already
  supplies the quotes. Write ``WHERE country = ${filter_country}`` instead. (This exact
  mistake shipped in this repo's own `spec_04`/`spec_06` fixtures until caught by a real
  pipeline run -- see the onboarding templates for the corrected form.)

* :func:`substitute_path_parameters` -- for **path/URI-shaped text** (an ingestion flow's
  ``source_config.path``/``schema_location``, a target's ``sink_config.path``/
  ``post_export_archive.output_zip_path``, and the reconciliation equivalents), applied to
  the raw ``source_config_json``/``target_config_json`` text before ``json.loads(...)``. Every
  value is rendered via plain ``str()`` -- **never** single-quoted -- since wrapping a path
  segment in SQL-style quotes would corrupt it (``/Volumes/x/'2026-08-29'/y`` is not a valid
  path). Do not reuse :func:`substitute_dynamic_parameters` for path text for exactly this
  reason.

Deliberately not wired into every path-shaped field in the schema: ``dq_config`` (DQ rule
``expr`` predicates) stays on the SQL-quoting renderer's territory if it ever needs
substitution at all, and ``observability_config.destination_config.volume_path`` is the one
field in the schema with a strict ``/Volumes/``-prefix format check at onboarding time -- a
``${param}``-*prefixed* value would newly fail that check, so it's left out of scope here
rather than silently half-supported.
"""

import re
from typing import Any, Dict

from flowx.lakeflow_framework.exceptions import FrameworkConfigError

_PLACEHOLDER_PATTERN = re.compile(r"\$\{(\w+)\}")


def substitute_dynamic_parameters(sql_text: str, parameters: Dict[str, Any]) -> str:
    """Substitute ``${param}`` placeholders in transformation SQL with resolved values.

    Values are rendered as SQL literals: strings are single-quoted (with embedded quotes
    escaped), everything else uses its Python ``str()`` representation.

    **Do not wrap ``${param}`` in quotes yourself in ``transformation_sql``** --
    ``WHERE country = '${filter_country}'`` becomes ``WHERE country = ''US''`` (a
    ``ParseException``) for a string parameter, because this function already quotes it.
    Write ``WHERE country = ${filter_country}`` instead; the substituted literal supplies
    its own quotes. (This exact mistake shipped in this repo's own `spec_04`/`spec_06`
    fixtures until caught by a real pipeline run -- see the onboarding templates for the
    corrected form.)

    Raises
    ------
    FrameworkConfigError
        If the SQL text references a placeholder not present in ``parameters``.
    """
    placeholders = set(_PLACEHOLDER_PATTERN.findall(sql_text))
    missing = placeholders.difference(parameters)
    if missing:
        raise FrameworkConfigError(f"Transformation SQL references undefined parameter(s): {sorted(missing)}")

    resolved_sql = sql_text
    for key, value in parameters.items():
        literal = f"'{str(value).replace(chr(39), chr(39) * 2)}'" if isinstance(value, str) else str(value)
        resolved_sql = resolved_sql.replace(f"${{{key}}}", literal)
    return resolved_sql


def substitute_path_parameters(text: str, parameters: Dict[str, Any]) -> str:
    """Substitute ``${param}`` placeholders in path/URI-shaped text with resolved values.

    Unlike :func:`substitute_dynamic_parameters`, every value is rendered via plain ``str()``
    -- never single-quoted -- since this is meant for text that ends up as a filesystem/Volume
    path or similar, not a SQL expression. Intended to run against the *raw JSON text* of a
    ``source_config_json``/``target_config_json`` column, before ``json.loads(...)``, so it
    applies uniformly to every string value the JSON contains without needing a recursive
    dict-walker.

    Raises
    ------
    FrameworkConfigError
        If ``text`` references a placeholder not present in ``parameters``.
    """
    placeholders = set(_PLACEHOLDER_PATTERN.findall(text))
    missing = placeholders.difference(parameters)
    if missing:
        raise FrameworkConfigError(f"Path text references undefined parameter(s): {sorted(missing)}")

    resolved_text = text
    for key, value in parameters.items():
        resolved_text = resolved_text.replace(f"${{{key}}}", str(value))
    return resolved_text
