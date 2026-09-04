"""MERGE INTO upserts of onboarded metadata into the control-spec tables (v2 schema).

Called from ``notebooks/02_onboarding/02_onboarding_engine.py`` once a submitted spec has passed
``onboarding/spec_validator.py::validate_spec`` -- this is the module that actually writes the
validated spec into ``dataflow_group_spec``/``ingestion_flow_spec``/``transformation_flow_spec``/
``reconciliation_flow_spec``, one upsert function per table, keyed on that table's natural id
(``dataflow_group_id``/``dataflow_id``/``flow_step_id``/``reconciliation_id`` respectively) so
re-onboarding the same flow updates its existing row instead of duplicating it. Every nested
config dict (``source_config``, ``target_config``, ``dq_config``, ``governance_tags``, etc.) is
stored pre-serialized as a ``..._json`` string column -- the control tables are queried by
``dataflow_group_id``/active-flag, never by reaching into a nested config field, so JSON storage
plus JSON-decode-at-read-time (in the engine notebook) is simpler than a fully normalized schema.
``cdc_load_strategy`` is the one exception, denormalized out of ``target_config`` onto its own
top-level column on both flow tables for fast SQL filtering/dispatch by the engine.

Each upsert function uses an explicit ``StructType`` (matching
``control_plane/ddl_definitions.py`` column-for-column) rather than letting
``spark.createDataFrame`` infer one from a list of ``Row`` objects: a batch of one or a few flows
can easily have an optional field (e.g. ``source_description``, ``transform_sql``) that is
``None`` for every row, and Spark has nothing to infer a type from in that case, raising
``CANNOT_DETERMINE_TYPE``. An explicit schema sidesteps inference entirely, for a batch of any
size.
"""

import datetime
import json
import logging
import re
from typing import Any, Dict, List

from delta.tables import DeltaTable
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType

from flowx.lakeflow_framework.exceptions import OnboardingUpsertError

logger = logging.getLogger("common.onboarding.metadata_upsert")

# Explicit schemas, matching control_plane/ddl_definitions.py. Required because a
# single-row (or all-flows-share-the-same-None-optional-field) DataFrame built from plain
# Row objects cannot have its schema inferred when an optional field is None for every row
# in the batch -- Spark raises CANNOT_DETERMINE_TYPE with no schema to fall back on. An
# explicit schema sidesteps inference entirely, for batches of any size.

_DATAFLOW_GROUP_SPEC_SCHEMA = StructType(
    [
        StructField("dataflow_group_id", StringType(), nullable=False),
        StructField("environment", StringType(), nullable=False),
        StructField("catalog_name", StringType(), nullable=False),
        StructField("has_ingestion_flows", BooleanType(), nullable=False),
        StructField("has_transformation_flows", BooleanType(), nullable=False),
        StructField("pipeline_parameters_json", StringType(), nullable=True),
        StructField("spark_config_json", StringType(), nullable=True),
        StructField("source_plane_config_json", StringType(), nullable=True),
        StructField("is_active", BooleanType(), nullable=False),
        StructField("created_at", TimestampType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

_INGESTION_FLOW_SPEC_SCHEMA = StructType(
    [
        StructField("dataflow_id", StringType(), nullable=False),
        StructField("dataflow_group_id", StringType(), nullable=False),
        StructField("source_system", StringType(), nullable=True),
        StructField("source_database", StringType(), nullable=True),
        StructField("source_table_name", StringType(), nullable=True),
        StructField("source_description", StringType(), nullable=True),
        StructField("source_type", StringType(), nullable=False),
        StructField("target_catalog", StringType(), nullable=False),
        StructField("target_schema", StringType(), nullable=False),
        StructField("target_table", StringType(), nullable=False),
        StructField("target_type", StringType(), nullable=False),
        StructField("cdc_load_strategy", StringType(), nullable=False),
        StructField("source_config_json", StringType(), nullable=True),
        StructField("target_config_json", StringType(), nullable=True),
        StructField("dq_config_json", StringType(), nullable=True),
        StructField("governance_tags_json", StringType(), nullable=True),
        StructField("is_active", BooleanType(), nullable=False),
        StructField("created_at", TimestampType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

_OBSERVABILITY_CONFIG_SCHEMA = StructType(
    [
        StructField("config_id", StringType(), nullable=False),
        StructField("dataflow_group_id", StringType(), nullable=False),
        StructField("destination_id", StringType(), nullable=False),
        StructField("enabled", BooleanType(), nullable=False),
        StructField("destination_type", StringType(), nullable=False),
        # Nullable on purpose: an observability_config row provisioned before v1.3.0 has no
        # mode, and observability/config_loader.py resolves NULL to DEFAULT_DESTINATION_MODE
        # ("triggered"), so pre-existing rows keep working with no migration.
        StructField("mode", StringType(), nullable=True),
        StructField("destination_config_json", StringType(), nullable=False),
        StructField("auth_config_json", StringType(), nullable=True),
        StructField("retry_config_json", StringType(), nullable=True),
        StructField("created_at", TimestampType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

_RECONCILIATION_FLOW_SPEC_SCHEMA = StructType(
    [
        StructField("reconciliation_id", StringType(), nullable=False),
        StructField("dataflow_group_id", StringType(), nullable=True),
        StructField("source_config_json", StringType(), nullable=False),
        StructField("target_configs_json", StringType(), nullable=False),
        StructField("match_keys_json", StringType(), nullable=False),
        StructField("compare_columns_json", StringType(), nullable=True),
        StructField("transform_sql", StringType(), nullable=True),
        StructField("error_handling_json", StringType(), nullable=True),
        StructField("logging_config_json", StringType(), nullable=True),
        StructField("two_tier_verification", BooleanType(), nullable=True),
        StructField("execution_mode", StringType(), nullable=True),
        StructField("publish_schema", StringType(), nullable=True),
        StructField("dq_config_json", StringType(), nullable=True),
        StructField("is_active", BooleanType(), nullable=False),
        StructField("created_at", TimestampType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

_TRANSFORMATION_FLOW_SPEC_SCHEMA = StructType(
    [
        StructField("flow_step_id", StringType(), nullable=False),
        StructField("dataflow_id", StringType(), nullable=False),
        StructField("dataflow_group_id", StringType(), nullable=False),
        StructField("target_catalog", StringType(), nullable=False),
        StructField("target_schema", StringType(), nullable=False),
        StructField("target_table", StringType(), nullable=False),
        StructField("target_type", StringType(), nullable=False),
        StructField("cdc_load_strategy", StringType(), nullable=False),
        StructField("source_inputs_json", StringType(), nullable=True),
        StructField("transformation_sql", StringType(), nullable=False),
        StructField("target_config_json", StringType(), nullable=True),
        StructField("dq_config_json", StringType(), nullable=True),
        StructField("governance_tags_json", StringType(), nullable=True),
        StructField("is_active", BooleanType(), nullable=False),
        StructField("created_at", TimestampType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)


def upsert_dataflow_group_spec(
    spark: SparkSession,
    control_schema: str,
    spec: Dict[str, Any],
    ingestion_flows: List[Dict[str, Any]],
    transformation_flows: List[Dict[str, Any]],
    catalog: str,
    environment: str,
) -> None:
    """Upsert the single ``dataflow_group_spec`` row for this onboarding spec.

    Raises
    ------
    OnboardingUpsertError
        If the MERGE fails (malformed spec, missing target table, etc.).
    """
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        group_row = Row(
            dataflow_group_id=spec["dataflow_group_id"],
            environment=environment,
            catalog_name=catalog,
            has_ingestion_flows=bool(ingestion_flows),
            has_transformation_flows=bool(transformation_flows),
            pipeline_parameters_json=json.dumps(spec.get("pipeline_parameters", {})),
            spark_config_json=json.dumps(spec.get("spark_config", {})),
            source_plane_config_json=json.dumps(spec.get("source_plane", {})),
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        source_df = spark.createDataFrame([group_row], schema=_DATAFLOW_GROUP_SPEC_SCHEMA)
        target = DeltaTable.forName(spark, f"{control_schema}.dataflow_group_spec")
        (
            target.alias("t")
            .merge(source_df.alias("s"), "t.dataflow_group_id = s.dataflow_group_id")
            .whenMatchedUpdate(
                set={
                    "environment": "s.environment",
                    "catalog_name": "s.catalog_name",
                    "has_ingestion_flows": "s.has_ingestion_flows",
                    "has_transformation_flows": "s.has_transformation_flows",
                    "pipeline_parameters_json": "s.pipeline_parameters_json",
                    "spark_config_json": "s.spark_config_json",
                    "source_plane_config_json": "s.source_plane_config_json",
                    "is_active": "s.is_active",
                    "updated_at": "s.updated_at",
                }
            )
            # whenNotMatchedInsert(values=...), scoped to source_df.columns -- not
            # whenNotMatchedInsertAll(), which resolves against the FULL current target schema
            # (a star-insert) and fails with DELTA_MERGE_UNRESOLVED_EXPRESSION the moment the
            # target table has a column this source_df doesn't carry (confirmed live: this
            # bit ingestion_flow_spec/transformation_flow_spec/reconciliation_flow_spec the
            # instant a new nullable column was added to their DDL and a fresh, never-before-
            # onboarded flow_id/reconciliation_id was inserted). Every whenMatchedUpdate above
            # already only ever touches source_df.columns for the same reason -- this makes the
            # insert side consistent with it, and future column additions here need no
            # corresponding change to this function.
            .whenNotMatchedInsert(values={col: f"s.{col}" for col in source_df.columns})
            .execute()
        )
    except Exception as exc:  # noqa: BLE001
        raise OnboardingUpsertError(f"Failed to upsert dataflow_group_spec: {exc}") from exc


def upsert_ingestion_flow_spec(
    spark: SparkSession, control_schema: str, group_id: str, ingestion_flows: List[Dict[str, Any]]
) -> None:
    """Upsert one row per ingestion flow into ``ingestion_flow_spec``.

    ``cdc_load_strategy`` is extracted from ``target_config.cdc_load_strategy`` (the CDC
    settings live inside ``target_config`` in the v2 spec shape) and denormalized onto its own
    top-level column for fast SQL filtering/dispatch by the engine.

    Raises
    ------
    OnboardingUpsertError
        If a flow is missing a required key, ``target_config.cdc_load_strategy`` is missing, or
        the MERGE fails.
    """
    if not ingestion_flows:
        return
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        rows = [
            Row(
                dataflow_id=flow["dataflow_id"],
                dataflow_group_id=group_id,
                source_system=flow.get("source_system"),
                source_database=flow.get("source_database"),
                source_table_name=flow.get("source_table_name"),
                source_description=flow.get("source_description"),
                source_type=flow["source_type"],
                target_catalog=flow["target_catalog"],
                target_schema=flow["target_schema"],
                target_table=flow["target_table"],
                target_type=flow["target_type"],
                cdc_load_strategy=flow.get("target_config", {})["cdc_load_strategy"],
                source_config_json=json.dumps(flow.get("source_config", {})),
                target_config_json=json.dumps(flow.get("target_config", {})),
                dq_config_json=json.dumps(flow.get("dq_config", {})),
                governance_tags_json=json.dumps(flow.get("governance_tags", {})),
                is_active=True,
                created_at=now,
                updated_at=now,
            )
            for flow in ingestion_flows
        ]
        source_df = spark.createDataFrame(rows, schema=_INGESTION_FLOW_SPEC_SCHEMA)
        target = DeltaTable.forName(spark, f"{control_schema}.ingestion_flow_spec")
        (
            target.alias("t")
            .merge(source_df.alias("s"), "t.dataflow_id = s.dataflow_id")
            .whenMatchedUpdate(set={col: f"s.{col}" for col in source_df.columns if col not in ("dataflow_id", "created_at")})
            .whenNotMatchedInsert(values={col: f"s.{col}" for col in source_df.columns})
            .execute()
        )
    except KeyError as exc:
        raise OnboardingUpsertError(f"Ingestion flow missing required key {exc} (note: cdc_load_strategy now lives under target_config)") from exc
    except Exception as exc:  # noqa: BLE001
        raise OnboardingUpsertError(f"Failed to upsert ingestion_flow_spec: {exc}") from exc


def upsert_transformation_flow_spec(
    spark: SparkSession, control_schema: str, group_id: str, transformation_flows: List[Dict[str, Any]]
) -> None:
    """Upsert one row per transformation flow into ``transformation_flow_spec``.

    Raises
    ------
    OnboardingUpsertError
        If a flow is missing a required key, ``target_config.cdc_load_strategy`` is missing, or
        the MERGE fails.
    """
    if not transformation_flows:
        return
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        rows = [
            Row(
                flow_step_id=flow["flow_step_id"],
                dataflow_id=flow["dataflow_id"],
                dataflow_group_id=group_id,
                target_catalog=flow["target_catalog"],
                target_schema=flow["target_schema"],
                target_table=flow["target_table"],
                target_type=flow["target_type"],
                cdc_load_strategy=flow.get("target_config", {})["cdc_load_strategy"],
                source_inputs_json=json.dumps(flow.get("source_inputs", [])),
                transformation_sql=flow["transformation_sql"],
                target_config_json=json.dumps(flow.get("target_config", {})),
                dq_config_json=json.dumps(flow.get("dq_config", {})),
                governance_tags_json=json.dumps(flow.get("governance_tags", {})),
                is_active=True,
                created_at=now,
                updated_at=now,
            )
            for flow in transformation_flows
        ]
        source_df = spark.createDataFrame(rows, schema=_TRANSFORMATION_FLOW_SPEC_SCHEMA)
        target = DeltaTable.forName(spark, f"{control_schema}.transformation_flow_spec")
        (
            target.alias("t")
            .merge(source_df.alias("s"), "t.flow_step_id = s.flow_step_id")
            .whenMatchedUpdate(set={col: f"s.{col}" for col in source_df.columns if col not in ("flow_step_id", "created_at")})
            .whenNotMatchedInsert(values={col: f"s.{col}" for col in source_df.columns})
            .execute()
        )
    except KeyError as exc:
        raise OnboardingUpsertError(f"Transformation flow missing required key {exc} (note: cdc_load_strategy now lives under target_config)") from exc
    except Exception as exc:  # noqa: BLE001
        raise OnboardingUpsertError(f"Failed to upsert transformation_flow_spec: {exc}") from exc


def upsert_reconciliation_flow_spec(
    spark: SparkSession, control_schema: str, group_id: str, reconciliation_flows: List[Dict[str, Any]]
) -> None:
    """Upsert one row per reconciliation flow into ``reconciliation_flow_spec``.

    ``target_configs`` (plural, a list) replaces the old singular ``cdc_config`` -- one
    reconciliation flow can compare its ``source_config`` against multiple targets, each with
    its own ``append_target_table`` (see ``reconciliation/matcher.py``).

    Raises
    ------
    OnboardingUpsertError
        If a flow is missing a required key or the MERGE fails.
    """
    if not reconciliation_flows:
        return
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        rows = [
            Row(
                reconciliation_id=flow["reconciliation_id"],
                # The flow's OWN dataflow_group_id wins over the spec's top-level one, falling
                # back to it when absent. v1.5.0 made this a real per-flow attribute: V-CYC-6
                # REQUIRES it on a pipeline-mode flow, and its rejection message explicitly offers
                # "another group's, to run inside that group's pipeline instead" as a supported
                # choice. Hardcoding group_id here -- as this line did until the fix -- silently
                # discarded that choice and registered the flow into the spec's own pipeline
                # instead, which is the same validator-accepts-it-but-upsert-drops-it defect class
                # that lost execution_mode. `or` rather than a None check is deliberate: an empty
                # string is not a usable group id either, and check_string has already rejected a
                # non-string.
                dataflow_group_id=flow.get("dataflow_group_id") or group_id,
                source_config_json=json.dumps(flow["source_config"]),
                target_configs_json=json.dumps(flow["target_configs"]),
                match_keys_json=json.dumps(flow["match_keys"]),
                compare_columns_json=json.dumps(flow.get("compare_columns", [])),
                transform_sql=flow.get("transform_sql"),
                error_handling_json=json.dumps(flow.get("error_handling", {})),
                logging_config_json=json.dumps(flow.get("logging_config", {})),
                # Persisted as written, with None left as SQL NULL rather than coerced to a
                # literal default: the DDL documents NULL as the default for each of these
                # (two_tier_verification -> true, execution_mode -> "job"), and writing the
                # default explicitly would make a later change of default invisible to already
                # onboarded rows. Omitting them entirely -- as this Row literal did until
                # v1.5.0 -- silently dropped the operator's value, which for execution_mode
                # meant a "pipeline" flow was read back as "job" and never entered the DAG.
                two_tier_verification=flow.get("two_tier_verification"),
                execution_mode=flow.get("execution_mode"),
                publish_schema=flow.get("publish_schema"),
                dq_config_json=json.dumps(flow["dq_config"]) if flow.get("dq_config") else None,
                is_active=True,
                created_at=now,
                updated_at=now,
            )
            for flow in reconciliation_flows
        ]
        source_df = spark.createDataFrame(rows, schema=_RECONCILIATION_FLOW_SPEC_SCHEMA)
        target = DeltaTable.forName(spark, f"{control_schema}.reconciliation_flow_spec")
        (
            target.alias("t")
            .merge(source_df.alias("s"), "t.reconciliation_id = s.reconciliation_id")
            .whenMatchedUpdate(
                set={col: f"s.{col}" for col in source_df.columns if col not in ("reconciliation_id", "created_at")}
            )
            .whenNotMatchedInsert(values={col: f"s.{col}" for col in source_df.columns})
            .execute()
        )
    except KeyError as exc:
        raise OnboardingUpsertError(f"Reconciliation flow missing required key {exc}") from exc
    except Exception as exc:  # noqa: BLE001
        raise OnboardingUpsertError(f"Failed to upsert reconciliation_flow_spec: {exc}") from exc


_UNSAFE_CONFIG_ID_CHARS = re.compile(r"[^A-Za-z0-9_.-]")


def upsert_observability_config(
    spark: SparkSession, control_schema: str, group_id: str, observability_destinations: List[Dict[str, Any]]
) -> None:
    """Upsert one row per telemetry destination into ``observability_config``, scoped to
    ``group_id`` -- from the ``observability[]`` array in the *same* onboarding spec as every
    other flow, not a separate config file (see ``docs/25_dlt_observability_module.md``).

    ``config_id`` is deterministically derived from ``(group_id, destination_id)`` rather than
    a fresh random UUID per call, so re-onboarding the same spec ``MERGE``s the existing row
    instead of accumulating duplicates -- the same idempotency guarantee every other upsert
    function here gets for free from a caller-supplied natural key, applied here since
    ``destination_id`` alone isn't guaranteed globally unique across dataflow groups.

    Raises
    ------
    OnboardingUpsertError
        If a destination is missing a required key or the MERGE fails.
    """
    if not observability_destinations:
        return
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        rows = []
        for destination in observability_destinations:
            destination_id = destination["id"]
            safe_group = _UNSAFE_CONFIG_ID_CHARS.sub("_", group_id)
            safe_destination = _UNSAFE_CONFIG_ID_CHARS.sub("_", destination_id)
            retry_config = dict(destination.get("retry") or {})
            if destination.get("timeout_ms") is not None:
                retry_config["timeout_ms"] = destination["timeout_ms"]

            rows.append(
                Row(
                    config_id=f"obscfg_{safe_group}_{safe_destination}",
                    dataflow_group_id=group_id,
                    destination_id=destination_id,
                    enabled=bool(destination.get("enabled", True)),
                    destination_type=destination["type"],
                    # Persist the spec's mode verbatim (None when absent). Without this the
                    # column stayed NULL for every onboarded destination and config_loader.py
                    # resolved every one of them to "triggered" -- making a spec-declared
                    # "mode": "continuous" destination silently unreachable by the continuous
                    # streaming engine, which filters on exactly this column.
                    mode=destination.get("mode"),
                    destination_config_json=json.dumps(destination["destination_config"]),
                    auth_config_json=json.dumps(destination["auth"]) if destination.get("auth") else None,
                    retry_config_json=json.dumps(retry_config) if retry_config else None,
                    created_at=now,
                    updated_at=now,
                )
            )
        source_df = spark.createDataFrame(rows, schema=_OBSERVABILITY_CONFIG_SCHEMA)
        target = DeltaTable.forName(spark, f"{control_schema}.observability_config")
        (
            target.alias("t")
            .merge(source_df.alias("s"), "t.config_id = s.config_id")
            .whenMatchedUpdate(set={col: f"s.{col}" for col in source_df.columns if col not in ("config_id", "created_at")})
            .whenNotMatchedInsert(values={col: f"s.{col}" for col in source_df.columns})
            .execute()
        )
    except KeyError as exc:
        raise OnboardingUpsertError(f"Observability destination missing required key {exc}") from exc
    except Exception as exc:  # noqa: BLE001
        raise OnboardingUpsertError(f"Failed to upsert observability_config: {exc}") from exc
