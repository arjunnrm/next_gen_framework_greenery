"""Unit tests for transformation/parameters.py -- pure Python, no Spark needed."""

import pytest

from flowx.lakeflow_framework.exceptions import FrameworkConfigError
from flowx.lakeflow_framework.transformation.parameters import (
    substitute_dynamic_parameters,
    substitute_path_parameters,
)


def test_substitutes_string_parameter_as_quoted_literal():
    resolved = substitute_dynamic_parameters("SELECT * FROM t WHERE region = ${region}", {"region": "US"})
    assert resolved == "SELECT * FROM t WHERE region = 'US'"


def test_substitutes_numeric_parameter_without_quotes():
    resolved = substitute_dynamic_parameters("SELECT * FROM t WHERE n > ${threshold}", {"threshold": 42})
    assert resolved == "SELECT * FROM t WHERE n > 42"


def test_substitutes_multiple_distinct_placeholders():
    resolved = substitute_dynamic_parameters("${a} AND ${b}", {"a": "x", "b": "y"})
    assert resolved == "'x' AND 'y'"


def test_escapes_embedded_single_quotes_in_string_values():
    resolved = substitute_dynamic_parameters("${name}", {"name": "O'Brien"})
    assert resolved == "'O''Brien'"


def test_undefined_placeholder_raises_framework_config_error():
    with pytest.raises(FrameworkConfigError, match="undefined parameter"):
        substitute_dynamic_parameters("SELECT * FROM t WHERE x = ${missing}", {})


def test_sql_with_no_placeholders_is_unchanged():
    sql = "SELECT * FROM t"
    assert substitute_dynamic_parameters(sql, {"unused": "value"}) == sql


def test_repeated_placeholder_substituted_everywhere():
    resolved = substitute_dynamic_parameters("${x} = ${x}", {"x": "US"})
    assert resolved == "'US' = 'US'"


def test_quoting_a_string_placeholder_yourself_produces_double_quotes():
    """Regression guard for the double-quoting bug that shipped in this repo's own
    spec_04/spec_06 fixtures: ``'${param}'`` in transformation_sql is wrong for a string
    parameter, since this function already quotes it. This test documents *why* --
    onboarding specs must write ``${param}`` unquoted (see the function's docstring)."""
    resolved = substitute_dynamic_parameters("WHERE country = '${filter_country}'", {"filter_country": "US"})
    assert resolved == "WHERE country = ''US''"  # the double quotes ARE the bug -- don't do this
    correct = substitute_dynamic_parameters("WHERE country = ${filter_country}", {"filter_country": "US"})
    assert correct == "WHERE country = 'US'"


# --- substitute_path_parameters: plain (unquoted) rendering for path/URI-shaped text ---


def test_path_substitutes_string_parameter_without_quotes():
    resolved = substitute_path_parameters("/Volumes/x/${run_date}/data", {"run_date": "2026-08-29"})
    assert resolved == "/Volumes/x/2026-08-29/data"  # no surrounding quotes, unlike the SQL variant


def test_path_substitutes_numeric_parameter():
    resolved = substitute_path_parameters("/data/batch_${n}", {"n": 5})
    assert resolved == "/data/batch_5"


def test_path_with_no_placeholders_is_unchanged():
    path = "/Volumes/catalog/landing/data"
    assert substitute_path_parameters(path, {"unused": "value"}) == path


def test_path_undefined_placeholder_raises_framework_config_error():
    with pytest.raises(FrameworkConfigError, match="undefined parameter"):
        substitute_path_parameters("/Volumes/x/${missing}/data", {})


def test_path_repeated_placeholder_substituted_everywhere():
    resolved = substitute_path_parameters("/${env}/data/${env}", {"env": "dev"})
    assert resolved == "/dev/data/dev"


def test_path_does_not_quote_a_string_value_unlike_sql_variant():
    """The whole reason a separate renderer exists: reusing substitute_dynamic_parameters
    for a path would inject literal quote characters, corrupting it."""
    path_result = substitute_path_parameters("${env}", {"env": "dev"})
    sql_result = substitute_dynamic_parameters("${env}", {"env": "dev"})
    assert path_result == "dev"
    assert sql_result == "'dev'"
