"""Concurrent control-table/function creation must not fail a job.

Every `flowx_test_*` job begins with `setup_control_tables`, so running any two of them at once
runs that provisioning concurrently. Unity Catalog's ``CREATE OR REPLACE FUNCTION`` is idempotent
in *intent* but not atomic: the loser of a race still gets ``[ROUTINE_ALREADY_EXISTS]``. Observed
live on 2026-08-29 -- three concurrent test jobs, one died with

    Cannot create the routine `flowx`.`config`.`preflight_check_onboarding_spec`
    because a routine of that name already exists

and every downstream task was skipped (TC-CDC-007 reported a framework failure that was really a
harness race).

The tolerance has to stay narrow, and that is what these tests defend: "someone else already made
it" is success, but a permission failure, a missing schema, or a syntax error in the function body
mean the object is genuinely NOT in the required state and must still fail loudly.
"""

import pytest

from flowx.lakeflow_framework.control_plane.schema_provisioner import (
    is_already_exists_race,
)


@pytest.mark.parametrize("message", [
    "[ROUTINE_ALREADY_EXISTS] Cannot create the routine `flowx`.`config`.`preflight_check_onboarding_spec`",
    "org.apache.spark.sql.catalyst.analysis.FunctionAlreadyExistsException",
    "[TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `x` because it already exists",
    "[SCHEMA_ALREADY_EXISTS] Cannot create schema `config` because it already exists",
])
def test_concurrent_creation_is_tolerated(message):
    assert is_already_exists_race(Exception(message)) is True


@pytest.mark.parametrize("message", [
    "PERMISSION_DENIED: User does not have CREATE FUNCTION on schema `config`",
    "[SCHEMA_NOT_FOUND] The schema `flowx`.`config` cannot be found",
    "PARSE_SYNTAX_ERROR: Syntax error at or near 'AS'",
    "[QUOTA_EXCEEDED.UC_RESOURCE_QUOTA_EXCEEDED] too many schemas",
    "Connection reset by peer",
])
def test_genuine_failures_still_propagate(message):
    assert is_already_exists_race(Exception(message)) is False


def test_matching_is_case_insensitive():
    """Spark renders these conditions in several casings across CLI, JVM and Python surfaces."""
    assert is_already_exists_race(Exception("routine_already_exists")) is True
