"""Unit tests for storage/table_properties.py -- pure Python, no Spark needed.

Regression coverage for a real bug found via live deployment: row-level Auto TTL was
completely non-functional. The framework set a nonexistent `delta.autoTTL.duration`
table property (Delta silently ignores unrecognized custom keys -- no error, ever) and
never captured the one thing Auto TTL cannot work without: which column determines a
row's age. See `storage/table_properties.py`'s module docstring for the full writeup.
"""

import pytest

from flowx.lakeflow_framework.exceptions import FrameworkConfigError
from flowx.lakeflow_framework.storage.table_properties import (
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
    assert properties["delta.enableIcebergCompatV2"] == "true"
    assert properties["delta.columnMapping.mode"] == "name"


def test_table_properties_enable_iceberg_read_uniformity_sets_uniform_property():
    properties = build_table_properties({"table_properties": {"enable_iceberg_read_uniformity": True}})
    assert properties["delta.universalFormat.enabledFormats"] == "iceberg"
    assert properties["delta.enableIcebergCompatV2"] == "true"
    assert properties["delta.columnMapping.mode"] == "name"


def test_storage_format_delta_does_not_set_uniform_property():
    properties = build_table_properties({"storage_format": "delta"})
    assert "delta.universalFormat.enabledFormats" not in properties
    assert "delta.enableIcebergCompatV2" not in properties
    assert "delta.columnMapping.mode" not in properties


def test_uniform_iceberg_always_sets_both_properties_together():
    """Regression test for the UC3 Phase C failure.

    Delta REJECTS ``delta.universalFormat.enabledFormats='iceberg'`` unless IcebergCompat is
    also explicitly enabled, with::

        [DELTA_UNIVERSAL_FORMAT_VIOLATION] The validation of Universal Format (iceberg) has
        failed: Requires IcebergCompat to be explicitly enabled in order for Universal Format
        (Iceberg) to be enabled on an existing table.

    Emitting one without the other is therefore never a valid configuration -- it is a pipeline
    update that aborts at runtime, which is exactly what happened to every UC3 bronze table on
    the first real use of this feature. All THREE properties are asserted as an inseparable TRIO
    (all-or-nothing) rather than individually, so a future edit cannot drop one and still pass
    this file. Delta reports only the NEXT missing property once the previous is satisfied, so
    each one here cost a separate failed pipeline update to discover."""
    for target_config in (
        {"storage_format": "iceberg"},
        {"table_properties": {"enable_iceberg_read_uniformity": True}},
        # every CDC strategy also emits CDF; the pair must survive alongside it
        {"table_properties": {"enable_iceberg_read_uniformity": True}, "cdc_load_strategy": "SCD2"},
    ):
        properties = build_table_properties(target_config)
        present = {
            key: key in properties
            for key in (
                "delta.universalFormat.enabledFormats",
                "delta.enableIcebergCompatV2",
                "delta.columnMapping.mode",
            )
        }
        assert len(set(present.values())) == 1, (
            f"UniForm properties must be emitted as an all-or-nothing TRIO, got {present} "
            f"for {target_config}"
        )
        assert properties["delta.enableIcebergCompatV2"] == "true"
        assert properties["delta.universalFormat.enabledFormats"] == "iceberg"
        assert properties["delta.columnMapping.mode"] == "name"


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


# ---------------------------------------------------------------------------------------------
# Iceberg read access: V2 for batch_table, V3 for pipeline-managed targets.
# ---------------------------------------------------------------------------------------------


def test_pipeline_managed_targets_get_icebergcompat_V3_not_V2():
    """A streaming table or MV MUST use V3 -- V2 cannot work on them at all.

    Databricks: "Iceberg reads can't be enabled on materialized views or streaming tables using
    IcebergCompatV2. However, for pipeline-managed materialized views and streaming tables, you
    can enable external Iceberg access using IcebergCompatV3 instead." Emitting V2 here is not
    merely suboptimal -- it fails the pipeline update outright, which is how UC3 discovered this.
    """
    for target_type in ("streaming_table", "materialized_view"):
        properties = build_table_properties(
            {"table_properties": {"enable_iceberg_read_uniformity": True}}, target_type
        )
        assert properties["delta.enableIcebergCompatV3"] == "true", target_type
        assert "delta.enableIcebergCompatV2" not in properties, target_type
        assert properties["delta.enableRowTracking"] == "true", target_type
        assert properties["pipelines.externalMetadata.enabled"] == "true", target_type
        assert properties["delta.columnMapping.mode"] == "name", target_type
        assert properties["delta.universalFormat.enabledFormats"] == "iceberg", target_type


def test_batch_table_keeps_icebergcompat_V2():
    properties = build_table_properties(
        {"table_properties": {"enable_iceberg_read_uniformity": True}}, "batch_table"
    )
    assert properties["delta.enableIcebergCompatV2"] == "true"
    assert "delta.enableIcebergCompatV3" not in properties
    assert "delta.enableRowTracking" not in properties
    assert "pipelines.externalMetadata.enabled" not in properties


def test_absent_target_type_defaults_to_V2_for_backward_compatibility():
    """``target_type`` is optional, so every pre-existing caller keeps its old behaviour."""
    properties = build_table_properties({"table_properties": {"enable_iceberg_read_uniformity": True}})
    assert properties["delta.enableIcebergCompatV2"] == "true"
    assert "delta.enableIcebergCompatV3" not in properties


def test_change_data_feed_is_suppressed_under_icebergcompat_V3():
    """CDF and IcebergCompatV3 are mutually exclusive -- Delta rejects a table carrying both.

    The explicit spec request (Iceberg read access) wins over the framework's own CDF
    convenience. The suppression is logged at WARNING naming the lost capability; it is not a
    silent ignore.
    """
    properties = build_table_properties(
        {"cdc_load_strategy": "SCD2", "table_properties": {"enable_iceberg_read_uniformity": True}},
        "streaming_table",
    )
    assert properties["delta.enableIcebergCompatV3"] == "true"
    assert "delta.enableChangeDataFeed" not in properties


def test_change_data_feed_survives_when_iceberg_is_not_requested():
    """The suppression is narrow: CDF is untouched for an ordinary CDC target."""
    properties = build_table_properties({"cdc_load_strategy": "SCD2"}, "streaming_table")
    assert properties["delta.enableChangeDataFeed"] == "true"


def test_change_data_feed_survives_alongside_V2_on_a_batch_table():
    """Only V3 conflicts with CDF; the V2 path must keep emitting it."""
    properties = build_table_properties(
        {"cdc_load_strategy": "SCD1", "table_properties": {"enable_iceberg_read_uniformity": True}},
        "batch_table",
    )
    assert properties["delta.enableIcebergCompatV2"] == "true"
    assert properties["delta.enableChangeDataFeed"] == "true"
