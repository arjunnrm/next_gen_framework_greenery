"""Perception-audit trail writer for `onboarding_audit_log`.

Called from ``notebooks/02_onboarding/02_onboarding_engine.py`` on **both** the success and
failure paths of an onboarding attempt -- an onboarding spec that fails validation or upsert
still leaves a row here, with ``status``/``error_message`` describing what went wrong -- so
every onboarding attempt, not just successful ones, is auditable after the fact. Fed by
``onboarding/client_context.py::build_client_context_json`` for the "who/what/where" perception
metadata (``client_context_json``) and by ``onboarding/spec_loader.py`` for ``spec_version``.

Uses an explicit ``StructType`` (:data:`_AUDIT_LOG_SCHEMA`, kept in step with
``control_plane/ddl_definitions.py::get_onboarding_audit_log_ddl``) rather than letting
``spark.createDataFrame`` infer one: a single-row DataFrame built from a plain ``Row`` cannot
have its schema inferred when an optional field (``error_message`` on the success path;
``dataflow_group_id`` if a spec's JSON failed to parse before it could even be read) is ``None``
-- Spark has nothing to infer a type from and raises ``CANNOT_DETERMINE_TYPE``. An explicit
schema sidesteps inference entirely.
"""

import datetime
import logging
import uuid
from typing import Optional

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import OnboardingUpsertError

logger = logging.getLogger("common.onboarding.audit_logger")

# Explicit schema, matching control_plane/ddl_definitions.py::get_onboarding_audit_log_ddl.
# Required because a single-row DataFrame built from a plain Row/dict cannot have its schema
# inferred when an optional field (e.g. error_message on the SUCCESS path, or
# dataflow_group_id if a spec's JSON failed to parse before it could be read) is None for
# every row in the batch -- Spark has nothing to infer a type from and raises
# CANNOT_DETERMINE_TYPE. An explicit schema sidesteps inference entirely.
_AUDIT_LOG_SCHEMA = StructType(
    [
        StructField("audit_event_id", StringType(), nullable=False),
        StructField("dataflow_group_id", StringType(), nullable=True),
        StructField("action_type", StringType(), nullable=False),
        StructField("environment", StringType(), nullable=True),
        StructField("onboarded_by", StringType(), nullable=True),
        StructField("onboarded_at", TimestampType(), nullable=False),
        StructField("spec_version", StringType(), nullable=True),
        StructField("client_context_json", StringType(), nullable=True),
        StructField("status", StringType(), nullable=False),
        StructField("error_message", StringType(), nullable=True),
        StructField("raw_spec_payload", StringType(), nullable=True),
    ]
)


def write_audit_log_entry(
    spark: SparkSession,
    control_schema: str,
    dataflow_group_id: Optional[str],
    action_type: str,
    environment: str,
    onboarded_by: str,
    spec_version: str,
    client_context_json: str,
    status: str,
    raw_spec_payload: str,
    error_message: Optional[str] = None,
) -> None:
    """Append one perception-audit row to ``onboarding_audit_log``.

    Called on both success and failure paths so every onboarding attempt -- not just
    successful ones -- leaves a trail.

    Raises
    ------
    OnboardingUpsertError
        If the append write fails.
    """
    try:
        audit_row = Row(
            audit_event_id=str(uuid.uuid4()),
            dataflow_group_id=dataflow_group_id,
            action_type=action_type,
            environment=environment,
            onboarded_by=onboarded_by,
            onboarded_at=datetime.datetime.now(datetime.timezone.utc),
            spec_version=spec_version,
            client_context_json=client_context_json,
            status=status,
            error_message=error_message,
            raw_spec_payload=raw_spec_payload,
        )
        spark.createDataFrame([audit_row], schema=_AUDIT_LOG_SCHEMA).write.format("delta").mode("append").saveAsTable(
            f"{control_schema}.onboarding_audit_log"
        )
    except Exception as exc:  # noqa: BLE001
        raise OnboardingUpsertError(f"Failed to write onboarding_audit_log entry: {exc}") from exc
