"""Unit tests for storage/table_properties.py -- pure Python, no Spark needed.

Regression coverage for a real bug found via live deployment: row-level Auto TTL was
completely non-functional. The framework set a nonexistent `delta.autoTTL.duration`
table property (Delta silently ignores unrecognized custom keys -- no error, ever) and
never captured the one thing Auto TTL cannot work without: which column determines a
row's age. See `storage/table_properties.py`'s module docstring for the full writeup.
"""

import pytest

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError
from NextGen_Metadata_Framework.lakeflow_framework.storage.table_properties import (
    build_auto_ttl_kwarg,
    build_table_properties,
)


def test_auto_ttl_absent_returns_none():
    assert build_auto_ttl_kwarg({}) is None


def test_auto_ttl_builds_correct_kwarg_dict():
    result = build_auto_ttl_kwarg({"auto_ttl": {"timestamp_column": "event_ts", "expire_in_days": 90}})
    assert result == {"timestamp_column": "event_ts", "expire_in_days": 90}


def test_auto_ttl_missing_timestamp_column_soft_skips_to_none():
    """auto_ttl is opt-in: an incomplete block returns None (skip TTL, log a warning)
    instead of raising -- mirrors spec_validator's matching soft-skip at onboarding time."""
    assert build_auto_ttl_kwarg({"auto_ttl": {"expire_in_days": 90}}) is None


def test_auto_ttl_missing_expire_in_days_soft_skips_to_none():
    assert build_auto_ttl_kwarg({"auto_ttl": {"timestamp_column": "event_ts"}}) is None


def test_auto_ttl_zero_expire_in_days_raises():
    with pytest.raises(FrameworkConfigError, match="positive integer"):
        build_auto_ttl_kwarg({"auto_ttl": {"timestamp_column": "event_ts", "expire_in_days": 0}})


def test_auto_ttl_negative_expire_in_days_raises():
    with pytest.raises(FrameworkConfigError, match="positive integer"):
        build_auto_ttl_kwarg({"auto_ttl": {"timestamp_column": "event_ts", "expire_in_days": -5}})


def test_auto_ttl_non_integer_expire_in_days_raises():
    with pytest.raises(FrameworkConfigError, match="positive integer"):
        build_auto_ttl_kwarg({"auto_ttl": {"timestamp_column": "event_ts", "expire_in_days": "90 days"}})


def test_auto_ttl_boolean_expire_in_days_rejected():
    """bool is a subclass of int in Python -- must not silently pass as a valid day count."""
    with pytest.raises(FrameworkConfigError, match="positive integer"):
        build_auto_ttl_kwarg({"auto_ttl": {"timestamp_column": "event_ts", "expire_in_days": True}})


def test_auto_ttl_unsafe_timestamp_column_rejected():
    with pytest.raises(FrameworkConfigError):
        build_auto_ttl_kwarg({"auto_ttl": {"timestamp_column": "event_ts; DROP TABLE x--", "expire_in_days": 90}})


def test_build_table_properties_never_sets_the_old_nonexistent_auto_ttl_property():
    """Regression guard: the old, broken `delta.autoTTL.duration` key must never reappear
    in table_properties -- Auto TTL is exclusively a decorator kwarg now (build_auto_ttl_kwarg),
    never a table property."""
    properties = build_table_properties({"auto_ttl_duration": "interval 90 days"})
    assert "delta.autoTTL.duration" not in properties
    assert not any(key.lower().startswith("delta.autottl") for key in properties)


def test_build_table_properties_unaffected_by_auto_ttl_field():
    """auto_ttl (the new field) must not leak into table_properties either -- it's handled
    entirely by build_auto_ttl_kwarg as a separate decorator kwarg."""
    properties = build_table_properties({"auto_ttl": {"timestamp_column": "event_ts", "expire_in_days": 90}})
    assert not any("ttl" in key.lower() for key in properties)


def test_storage_format_iceberg_sets_uniform_property():
    """v2 schema: storage_format='iceberg' (only valid for batch_table, enforced by the
    validator) maps to the same UniForm read-compatibility property as
    table_properties.enable_iceberg_read_uniformity -- Lakeflow tables have no separate
    native-Iceberg physical storage engine to opt into."""
    properties = build_table_properties({"storage_format": "iceberg"})
    assert properties["delta.universalFormat.enabledFormats"] == "iceberg"


def test_table_properties_enable_iceberg_read_uniformity_sets_uniform_property():
    properties = build_table_properties({"table_properties": {"enable_iceberg_read_uniformity": True}})
    assert properties["delta.universalFormat.enabledFormats"] == "iceberg"


def test_storage_format_delta_does_not_set_uniform_property():
    properties = build_table_properties({"storage_format": "delta"})
    assert "delta.universalFormat.enabledFormats" not in properties


def test_log_retention_duration_read_from_nested_table_properties():
    """v2 schema: log_retention_duration/deleted_file_retention_duration moved under
    target_config.table_properties (were top-level target_config fields in v1)."""
    properties = build_table_properties(
        {"table_properties": {"log_retention_duration": "interval 30 days", "deleted_file_retention_duration": "interval 14 days"}}
    )
    assert properties["delta.logRetentionDuration"] == "interval 30 days"
    assert properties["delta.deletedFileRetentionDuration"] == "interval 14 days"


def test_top_level_log_retention_duration_no_longer_read_v1_field_location_removed():
    """A stale v1-shaped target_config (log_retention_duration at the top level, not nested
    under table_properties) must not silently apply -- confirms the field genuinely moved,
    rather than both locations working by accident."""
    properties = build_table_properties({"log_retention_duration": "interval 30 days"})
    assert "delta.logRetentionDuration" not in properties


@pytest.mark.parametrize(
    "cdc_load_strategy",
    ["SCD1", "SCD2", "SCD3", "FULL_SNAPSHOT_CDC"],
)
def test_cdc_dispatched_strategies_enable_change_data_feed(cdc_load_strategy):
    """Every CDC-dispatched strategy needs delta.enableChangeDataFeed=true so
    cdc/change_metrics.py::capture_scd_change_counts can query table_changes() for exact
    inserted/updated/deleted row counts per pipeline update."""
    properties = build_table_properties({"cdc_load_strategy": cdc_load_strategy})
    assert properties["delta.enableChangeDataFeed"] == "true"


@pytest.mark.parametrize("cdc_load_strategy", ["APPEND", "TRUNCATE_AND_LOAD", None])
def test_no_op_strategies_do_not_enable_change_data_feed(cdc_load_strategy):
    """APPEND/TRUNCATE_AND_LOAD (and an unset strategy) have no CDC comparison concept --
    they must not pay the CDF storage/write overhead."""
    properties = build_table_properties({"cdc_load_strategy": cdc_load_strategy})
    assert "delta.enableChangeDataFeed" not in properties
