"""Structural, type, allowed-value, and SQL-syntax validation for onboarding specs (v2 schema).

Every problem found is recorded as one human-readable string of the form
``"<json_path>: <what's wrong>"`` -- e.g.::

    ingestion_flow[df_raw_txn].source_config.capture_technical_metadata: expected a
    boolean (true/false), got 'abc' (str)

``validate_spec`` collects *every* problem across the whole spec (not just the first one)
so a single onboarding attempt returns a complete report instead of a fix-one-rerun loop.

v2 schema, migrated from v1 (see docs/01_control_metadata_schema.md's Migration Notes for the
full list). The headline structural change: ``cdc_load_strategy`` and every CDC-related field
(``primary_keys``, ``sequence_by_column``, ``columns_to_check``, ``columns_to_exclude``,
``cdc_operation_column``/``cdc_operation_mapping``, ``generate_hash_columns``) now live
directly inside ``target_config`` -- there is no separate ``cdc_config`` sibling. DQ-related
fields (``rules``, ``quarantine_table``, ``record_id_column``) move into their own
``dq_config`` container. Governance moves to a tags-only model (``governance_tags``,
replacing ``abac_config``). ``source_format``/``depends_on_dataflow_group_ids`` are removed.
``source_type: "gcs_autoloader"`` is renamed to ``"autoloader"``. Every secret reference uses
the Unity Catalog three-level namespace (``secret_catalog``/``secret_schema``/``secret_key``).

v1.3.0 additions, and the three places this module got deliberately *more* permissive rather
than less (each one is a rule that used to reject a configuration an operator legitimately
wants):

- ``landing_retention_policy.archive_path`` is no longer required for
  ``clean_source == "archive"`` -- an empty/absent path degrades to a documented no-op at
  runtime instead of failing the pipeline update.
- ``partition_columns: []`` and ``liquid_clustering_columns: []`` are valid and mean "no
  partitioning"/"no clustering", never "misconfigured".

and the new fields: ``source_config.json_string_columns``/``remove_dups``/``dedup_watermark``/
``column_normalization``, ``source_zip_handling.delete_source_after_extract``'s action-object
form, ``target_config.empty_target_if_source_empty``, a ``liquid_clustering_columns`` count
limit, the top-level ``spark_config`` block, ``reconciliation_flows[].two_tier_verification``
plus a per-side ``task_run_id_column`` (and reconciliation narrowing to Delta tables only), and
``observability[].mode`` with its ``destination_config.event_log_tables``.

v1.4.0 removals -- five attributes and one CDC strategy, every one of them REJECTED rather than
ignored (see ``reject_removed_keys`` and the ``REMOVED_*`` registries for the exact migration
message each produces):

- ``source_config.normalize_column_names`` -> ``source_config.column_normalization.enabled``.
- ``target_config.generate_surrogate_key`` / ``surrogate_key_columns`` /
  ``surrogate_key_exclude_columns`` -> the surrogate-key engine is gone; declare ``primary_keys``.
- ``cdc_load_strategy: "FULL_SNAPSHOT_CDC_NO_PK"`` -> ``FULL_SNAPSHOT_CDC`` with ``primary_keys``
  (Databricks-native ``apply_changes_from_snapshot``), or ``TRUNCATE_AND_LOAD``.
- ``reconciliation_flows[].recon_mode`` / ``generate_surrogate_key`` -> reconciliation is
  triggered-only and matches on ``match_keys``.

and one addition: ``target_config.encrypted_columns[].source_data_type``, the declared original
type of an encrypted column (see :func:`_validate_encrypted_columns`).
"""

import difflib
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import SparkSession

from flowx.lakeflow_framework.engine.source_plane import MATERIALIZE_NEVER_REJECTION
from flowx.lakeflow_framework.exceptions import FrameworkConfigError
from flowx.lakeflow_framework.storage.table_properties import qualified_table_name
from flowx.lakeflow_framework.transformation.parameters import (
    substitute_dynamic_parameters,
    substitute_path_parameters,
)

logger = logging.getLogger("flowx.lakeflow_framework.onboarding.spec_validator")

ALLOWED_SOURCE_TYPES = {"autoloader", "zerobus", "asn1"}
ALLOWED_TARGET_TYPES = {"streaming_table", "materialized_view", "batch_table", "external_sink", "sink"}
# FULL_SNAPSHOT_CDC_NO_PK was REMOVED in v1.4.0 along with the surrogate-key engine it was the
# only mandatory consumer of. It is the one strategy that could not be expressed with a
# Databricks-native CDC pattern: `dlt.apply_changes_from_snapshot` requires `keys`, and with no
# natural key the framework had to manufacture one by hashing every payload column of every row
# on every run. Snapshot CDC now means FULL_SNAPSHOT_CDC with a real `primary_keys`; a source
# with genuinely no key uses TRUNCATE_AND_LOAD, which replaces the target wholesale and needs no
# key at all. See REMOVED_CDC_LOAD_STRATEGIES for the rejection message authors actually see.
ALLOWED_INGESTION_CDC_STRATEGIES = {
    "APPEND",
    "TRUNCATE_AND_LOAD",
    "SCD1",
    "SCD2",
    "FULL_SNAPSHOT_CDC",
}
ALLOWED_TRANSFORMATION_CDC_STRATEGIES = ALLOWED_INGESTION_CDC_STRATEGIES | {"SCD3"}
ALLOWED_DQ_ACTIONS = {"warn", "drop", "fail", "quarantine"}
ALLOWED_AES_MODES = {"GCM", "CBC", "ECB"}
ALLOWED_CLEAN_SOURCE_MODES = {"archive", "delete", "off"}
# source_zip_handling.delete_source_after_extract's OBJECT form. The legacy plain boolean is
# still accepted and normalized one layer down by ``archive/zip_utils.py::resolve_zip_delete_policy``
# (`true` -> {"action": "delete_now"}, `false` -> the internal-only "never" action). "never"
# deliberately has NO spec spelling of its own -- it exists only as the normalized form of the
# legacy `false` -- which is precisely why it is absent from this set: a spec author writing
# `{"action": "never"}` almost certainly meant the boolean `false` and should be told so.
ALLOWED_ZIP_DELETE_ACTIONS = {"delete_now", "delete_after_x_days"}
# source_config.column_normalization.case. Only the CASE FOLD is configurable: the character
# normalization itself (trim, replace [^A-Za-z0-9_] with '_', collapse repeated '_', strip edge
# '_') is always applied deterministically, and collision detection is always performed on the
# LOWERCASED projection of the produced names -- Unity Catalog and Spark resolve column
# references case-insensitively, so two output names differing only by case are an unusable
# table, not a valid one. See ingestion/column_normalization.py.
ALLOWED_COLUMN_NORMALIZATION_CASES = {"lower", "preserve", "upper"}
ALLOWED_SCHEMA_EVOLUTION_MODES = {"addNewColumns", "addNewColumnsWithTypeWidening", "rescue", "failOnNewColumns", "none"}
ALLOWED_STORAGE_FORMATS = {"delta", "iceberg"}
ALLOWED_SINK_FORMATS = {"delta", "kafka", "pgp_zip"}

#: ``sink_config.staged_file_format`` (v1.6.0) -- the staged per-partition file format inside
#: a ``pgp_zip`` archive. Absent == "json" (JSON-Lines, the only pre-v1.6.0 behaviour).
ALLOWED_STAGED_FILE_FORMATS = {"json", "csv"}
ALLOWED_SINK_WRITE_MODES = {"overwrite", "append"}
ALLOWED_RECONCILIATION_FAILURE_MODES = {"fail", "warn"}
# reconciliation_flows[].execution_mode -- "job" (default, unchanged standalone
# 05_reconciliation_engine.py job task), "pipeline" (register this flow's L3+L4 published
# datasets AND the L5 heal/append lane inside its dataflow_group_id's Lakeflow pipeline
# update), or "pipeline_audit_only" (register L3+L4 only -- comparison/metrics/dq_config run
# in-pipeline, healing stays on the standalone job engine). MUST default to "job": four DABs
# resources still run recon tasks against onboarded rows today, and "pipeline" defaulting on
# would run those flows twice per cycle. See control_plane/ddl_definitions.py's
# reconciliation_flow_spec.execution_mode column comment and docs/07_reconciliation_engine.md.
ALLOWED_RECONCILIATION_EXECUTION_MODES = {"job", "pipeline", "pipeline_audit_only"}
_RECONCILIATION_PIPELINE_MODES = {"pipeline", "pipeline_audit_only"}
# Narrowed from {"table", "file", "sink"}: reconciliation is Delta-tables-only. Both sides of a
# comparison must share one hash construction, one schema, and one restartability ledger, and a
# raw file/sink location can offer none of those -- read it into a Delta table first and
# reconcile against that table. ``reconciliation/dataset_reader.py`` re-asserts this at runtime
# (including that the resolved table's provider really is Delta) as defense in depth against a
# hand-edited control-table row.
ALLOWED_RECON_DATASET_TYPES = {"table"}
# ---------------------------------------------------------------------------------------------
# Attributes REMOVED in v1.4.0.
#
# Every one of these is rejected with an error rather than ignored. An ignored key is the worst
# of the three possible behaviours: the spec still onboards, the control table still gets a row,
# the pipeline still runs -- and it quietly does something other than what the document says. A
# removed key that used to switch a data-shaping behaviour ON (normalize_column_names,
# generate_surrogate_key) would silently switch it OFF, which is exactly the class of defect the
# validator exists to catch. So each maps to its own migration sentence, named per spec path.
# ---------------------------------------------------------------------------------------------
REMOVED_SOURCE_CONFIG_KEYS = {
    "normalize_column_names": (
        "removed in v1.4.0 -- column_normalization is now the only switch. Replace "
        "normalize_column_names: true with column_normalization: {enabled: true} (add a case "
        "key if you were relying on something other than the default 'lower'); delete the key "
        "outright if it was false."
    ),
}

REMOVED_TARGET_CONFIG_KEYS = {
    "generate_surrogate_key": (
        "removed in v1.4.0 -- the surrogate-key engine is gone. __framework_surrogate_key is no "
        "longer generated for any flow. Declare real primary_keys (SCD1/SCD2/SCD3/"
        "FULL_SNAPSHOT_CDC all take them), or use TRUNCATE_AND_LOAD if this source has no key."
    ),
    "surrogate_key_columns": (
        "removed in v1.4.0 with the surrogate-key engine -- it scoped a column that is no longer "
        "generated. To control what identifies a row, set primary_keys; to control what is "
        "compared for change, set columns_to_check/columns_to_exclude."
    ),
    "surrogate_key_exclude_columns": (
        "removed in v1.4.0 with the surrogate-key engine -- it scoped a column that is no longer "
        "generated. To control what identifies a row, set primary_keys; to control what is "
        "compared for change, set columns_to_check/columns_to_exclude."
    ),
}

REMOVED_RECONCILIATION_FLOW_KEYS = {
    "recon_mode": (
        "removed in v1.4.0 -- reconciliation is triggered-only. Every run is a bounded job task: "
        "batch reads, and trigger(availableNow=True) for a read_mode 'streaming' side, so the run "
        "drains its backlog and finishes. There is no continuous reconciliation mode and no "
        "recon_mode widget; delete the key. For continuous coverage, either schedule the "
        "reconciliation job on the cadence you need (execution_mode 'job', the default), or set "
        "execution_mode to 'pipeline'/'pipeline_audit_only' to run this flow's comparison inside "
        "its dataflow group's own Lakeflow pipeline update instead."
    ),
    "generate_surrogate_key": (
        "removed in v1.4.0 with the surrogate-key engine. A reconciliation flow matches on its "
        "declared match_keys; both sides must carry those columns."
    ),
}

# A removed VALUE of a surviving key, exactly like REMOVED_CDC_LOAD_STRATEGIES below -- NOT a
# REMOVED_*_KEYS entry. `materialize` itself is still a legal attribute ("always", "auto"); only
# the value "never" is prohibited. reject_removed_keys() triggers on KEY PRESENCE, so routing this
# through it would reject the perfectly legal `materialize: "always"` as well.
#
# Imported from engine/source_plane.py rather than re-typed, because the same value is rejected on
# two paths that must never drift: here at onboarding time, and inside plan_source_plane() at
# pipeline runtime for control-table rows persisted before the mandate (which onboarding
# validation never sees again).
REMOVED_MATERIALIZE_POLICIES = {
    "never": MATERIALIZE_NEVER_REJECTION,
}

REMOVED_CDC_LOAD_STRATEGIES = {
    "FULL_SNAPSHOT_CDC_NO_PK": (
        "removed in v1.4.0 -- it existed only to consume the surrogate-key engine, hashing every "
        "payload column of every row on every run to manufacture a diff key. Use FULL_SNAPSHOT_CDC "
        "with target_config.primary_keys (the Databricks-native apply_changes_from_snapshot "
        "pattern -- https://docs.databricks.com/aws/en/ldp/cdc), or TRUNCATE_AND_LOAD if this "
        "source genuinely has no key to diff on."
    ),
}


# ---------------------------------------------------------------------------
# Unknown-key rejection (v1.7.1)
# ---------------------------------------------------------------------------
# A key the framework does not read is not harmless: it onboards, writes its control-table row
# and runs the pipeline while doing nothing at all. `data_quality` instead of `dq_config` means
# DQ silently never runs; a v1-era `cdc_config` block means the CDC keys are silently ignored.
# This is the same failure mode REMOVED_*_KEYS exists to prevent, for keys that were never real
# in the first place -- overwhelmingly generated specs reconstructing field names from memory.
# Presence is the trigger, exactly as for a removed key.
#
# These sets are asserted equivalent to onboarding_templates/onboarding_spec.schema.json by
# tests/unit/test_unknown_key_rejection.py::test_allowed_key_sets_match_json_schema -- add an
# attribute in one place and that test fails until it is added in the other. They are declared
# here rather than read from the schema at import time because the schema file is not packaged
# into the wheel that runs on Databricks.
#
# Keys beginning with "_" are always allowed as author comments (JSON has no comment syntax and
# specs in flowx_testing/ rely on `_scenario` / `_test_case_note` / `_provenance`), as is the
# "$schema" editor hint.

ALLOWED_ROOT_KEYS = {
    "dataflow_group_id", "ingestion_flows", "observability", "pipeline_parameters",
    "reconciliation_flows", "source_plane", "spark_config", "transformation_flows"
}

#: The implicit default for reconciliation's two log-capture flags, mirroring layer 3 of
#: ``reconciliation/appender.py::resolve_log_capture_flags``. FALSE since v1.7.3 (was True):
#: reconciliation is SILENT BY DEFAULT and auditing is opt-in.
#:
#: This MUST stay equal to that resolver's fallback. Onboarding-time validation and graph-time
#: registration both branch on these flags, so a disagreement does not merely mis-report -- it
#: lets a spec pass review and then fail on its first pipeline update, which is exactly the
#: drift ``tests/unit/test_recon_logging_gates.py`` exists to prevent. It is asserted equal to
#: the runtime resolver by a test rather than imported, because importing the reconciliation
#: package here would pull Spark into onboarding-time validation.
_DEFAULT_LOG_CAPTURE = False

ALLOWED_SOURCE_PLANE_KEYS = {
    "catalog", "materialize", "schema"
}

#: Legal values for ``source_plane.materialize`` after v1.7.3's Single-Read mandate.
#:
#: ``"never"`` is absent deliberately and is NOT reported through this set -- it gets its own
#: named rejection from REMOVED_MATERIALIZE_POLICIES, so an author who used it is told it was
#: withdrawn and why, rather than the generic "not one of [always, auto]" list. Same reasoning,
#: and same ordering of the two checks, as REMOVED_CDC_LOAD_STRATEGIES.
#:
#: ``"auto"`` survives because it was never prohibited -- but it is no longer a distinct
#: behaviour: plan_source_plane resolves it to "always".
ALLOWED_MATERIALIZE_POLICIES = {"always", "auto"}

ALLOWED_INGESTION_FLOW_KEYS = {
    "dataflow_id", "dq_config", "governance_tags", "source_config", "source_database",
    "source_description", "source_system", "source_table_name", "source_type",
    "target_catalog", "target_config", "target_schema", "target_table", "target_type"
}

ALLOWED_TRANSFORMATION_FLOW_KEYS = {
    "dataflow_id", "dq_config", "flow_step_id", "governance_tags", "source_description",
    "source_inputs", "target_catalog", "target_config", "target_schema", "target_table",
    "target_type", "transformation_sql"
}

ALLOWED_INGESTION_SOURCE_CONFIG_KEYS = {
    "asn1_codec", "asn1_pdu_name", "asn1_schema_path", "auto_flatten_all",
    "capture_technical_metadata", "column_normalization", "data_standardization_sql",
    "dedup_watermark", "explode_columns", "file_pattern", "format", "json_string_columns",
    "landing_retention_policy", "max_bytes_per_trigger", "path", "reader_options",
    "remove_dups", "schema_config_path", "schema_evolution_mode", "schema_location",
    "source_catalog", "source_schema", "source_table", "source_zip_handling",
    "starting_version"
}

ALLOWED_TARGET_CONFIG_KEYS = {
    "auto_ttl", "capture_technical_metadata", "cdc_load_strategy", "cdc_operation_column",
    "cdc_operation_mapping", "columns_to_check", "columns_to_exclude",
    "empty_target_if_source_empty", "encrypted_columns", "generate_hash_columns",
    "liquid_clustering_columns", "partition_columns", "primary_keys", "sequence_by_column",
    "sink_config", "storage_format", "table_properties"
}

ALLOWED_DQ_CONFIG_KEYS = {
    "quarantine_table", "record_id_column", "rules"
}

ALLOWED_GOVERNANCE_TAGS_KEYS = {
    "column_tags", "table_tags"
}

ALLOWED_SOURCE_INPUT_KEYS = {
    "decrypted_columns", "input_name", "is_streaming", "table", "watermark"
}

ALLOWED_SINK_CONFIG_KEYS = {
    "export_trigger", "format", "kafka_options", "kafka_secret_options", "path",
    "post_export_archive", "staged_file_format", "staged_file_options", "write_mode"
}

#: ``sink_config.export_trigger`` (v1.7.5) -- WHAT drives a pgp_zip export.
#:
#: ``"per_micro_batch"`` (the default when absent, and the only pre-v1.7.5 behaviour) feeds the
#: sink from the flow's own staged view, so one archive is produced per micro-batch of an
#: append-only stream. That is correct for an append-only feed and IMPOSSIBLE for an
#: aggregating one: a materialized_view / TRUNCATE_AND_LOAD target is fully recomputed each
#: update, which Delta refuses to stream from at all (DELTA_SOURCE_TABLE_IGNORE_CHANGES), so
#: those targets had no export path whatsoever before v1.7.5.
#:
#: ``"per_update"`` decouples the two: the sink is driven by an update-scoped pulse rather than
#: by business rows, and the payload is read batch-side. Exactly one export per pipeline
#: update, including an update in which no new source rows arrived -- verified live across
#: three consecutive updates, the second and third of which ingested nothing.
ALLOWED_EXPORT_TRIGGERS = {"per_micro_batch", "per_update"}

#: ``sink_config.staged_file_options`` (v1.7.4) -- the staged CSV's dialect.
ALLOWED_STAGED_FILE_OPTIONS_KEYS = {"delimiter", "include_header", "line_terminator"}
ALLOWED_STAGED_LINE_TERMINATORS = {"crlf", "lf"}

#: ``post_export_archive.archive_format`` (v1.7.4) -- the finished archive's container.
ALLOWED_ARCHIVE_FORMATS = {"gzip", "zip"}

ALLOWED_RECONCILIATION_FLOW_KEYS = {
    "compare_columns", "dataflow_group_id", "dq_config", "error_handling", "execution_mode",
    "logging_config", "match_keys", "publish_schema", "reconciliation_id", "source_config",
    "target_configs", "transform_sql", "two_tier_verification"
}


# Known-wrong attribute names and what they should be, checked before difflib's fuzzy match.
# Every entry here is a name observed in a real generated spec: either a v1-era key that a model
# learned from an older document, or a plausible-sounding invention borrowed from another
# framework (Spark reader options, dbt, Delta Live Tables prose). difflib alone gets several of
# these wrong -- "cdc_config" is a closer string match to "dq_config" than to anything useful,
# and "infer_schema" matches "source_schema" -- so an explicit mapping beats a fuzzy guess.
UNKNOWN_KEY_ALIASES = {
    "depends_on_dataflow_group_ids": (
        "no replacement in the spec -- inter-group ordering is a Lakeflow Jobs concern. Express "
        "it as a task dependency (depends_on) between the groups' jobs in the bundle, not here. "
        "Until v1.7.1 this key was silently unread, so any ordering it appeared to declare was "
        "never actually enforced"
    ),
    "cdc_config": (
        "target_config.cdc_load_strategy (plus primary_keys / sequence_by_column alongside it) -- "
        "the separate cdc_config block was a v1 shape and no longer exists"
    ),
    "data_quality": "dq_config",
    "quality_config": "dq_config",
    "expectations": "dq_config.rules",
    "file_format": "source_config.format",
    "infer_schema": (
        "No replacement -- delete it. Auto Loader schema inference is always on; point "
        "schema_location at a writable path, or pin types explicitly with schema_config_path"
    ),
    "schema_inference": (
        "No replacement -- delete it; see schema_location / schema_config_path"
    ),
    "partition_by": "target_config.partition_columns",
    "cluster_by": "target_config.liquid_clustering_columns",
    "primary_key": "target_config.primary_keys (a list, even for a single column)",
    "merge_keys": "target_config.primary_keys",
    "sequence_by": "target_config.sequence_by_column",
    "tags": "governance_tags.table_tags",
    "table_comment": "target_config.table_properties",
    "normalize_columns": "source_config.column_normalization ({enabled, case})",
    "source_path": "source_config.path",
    "target_path": "target_config.sink_config.path (sink targets only)",
    "flow_id": "dataflow_id (ingestion) or flow_step_id (transformation)",
    "sql": "transformation_sql (transformation flows) or transform_sql (reconciliation flows)",
    "query": "transformation_sql",
}


def reject_unknown_keys(config: Any, path_prefix: str, errors: List[str], allowed: set) -> None:
    """Append one error per key on ``config`` the framework does not read.

    Author comments (any key starting with ``_``) and the ``$schema`` editor hint are exempt.
    When an unknown key is a near-miss for a real one, the message names the real one --
    ``dq_config`` for ``data_quality`` -- because the single most common source of these is a
    generated spec that reconstructed the field name from memory, and the fix is almost always
    a rename rather than a deletion.
    """
    if not isinstance(config, dict):
        return
    for key in config:
        if key.startswith("_") or key == "$schema" or key in allowed:
            continue
        path = f"{path_prefix}.{key}" if path_prefix else key
        # An alias is only a rename hint where the name is genuinely wrong. Some of these are
        # real keys in a DIFFERENT container -- observability[].destination_config.file_format
        # is legitimate -- so never offer the alias for a key the caller's own allowlist
        # accepts, and let the allowlist decide before the alias table does.
        if key in UNKNOWN_KEY_ALIASES:
            hint = f" Use {UNKNOWN_KEY_ALIASES[key]}." if not UNKNOWN_KEY_ALIASES[key].startswith("No replacement") else f" {UNKNOWN_KEY_ALIASES[key]}."
        else:
            suggestion = difflib.get_close_matches(key, sorted(allowed), n=1, cutoff=0.6)
            hint = (
                f" Did you mean {suggestion[0]!r}?"
                if suggestion
                else f" Allowed keys here: {', '.join(sorted(allowed))}."
            )
        errors.append(
            f"{path}: not a recognised attribute -- the framework never reads it, so "
            f"leaving it in place silently does nothing.{hint}"
        )


def reject_removed_keys(config: Any, path_prefix: str, errors: List[str], removed: Dict[str, str]) -> None:
    """Append one error per removed key present on ``config``.

    Presence alone is the trigger -- not truthiness. ``generate_surrogate_key: false`` is still a
    statement about a feature that no longer exists, and leaving it in a spec means the next
    person to read it believes the framework still has the knob. Reporting it costs the author one
    deletion and buys a document that describes what actually runs.
    """
    if not isinstance(config, dict):
        return
    for key, guidance in removed.items():
        if key in config:
            errors.append(f"{path_prefix}.{key}: {guidance}")


def reject_mode_incompatible_keys(
    config: Any, path_prefix: str, errors: List[str], incompatible: Dict[str, str], mode: str
) -> None:
    """Append one error per key present on ``config`` that is incompatible with the ``mode``
    the caller has already determined applies here -- e.g. a reconciliation dataset side's
    ``task_run_id_column`` when ``execution_mode`` is ``"pipeline"``/``"pipeline_audit_only"``,
    or a flow-level ``dq_config``/``publish_schema`` while ``execution_mode`` is (still) ``"job"``.

    Same convention as :func:`reject_removed_keys` -- presence alone is the trigger, not
    truthiness -- but unlike a removed key, nothing here is gone from the spec forever: the
    same key is perfectly valid for a different flow, or for this same flow under a different
    ``execution_mode``. Callers are expected to invoke this only once they know ``mode``
    actually applies (e.g. only when the flow is genuinely in a pipeline mode), so the message
    always names the mode that is actually in force, not merely one of several allowed values.
    """
    if not isinstance(config, dict):
        return
    for key, guidance in incompatible.items():
        if key in config:
            errors.append(f"{path_prefix}.{key}: not supported when execution_mode is {mode!r}. {guidance}")


ALLOWED_COMPARISON_DIRECTIONS = {"source_to_target", "target_to_source", "both"}
ALLOWED_READ_MODES = {"batch", "streaming"}
# Presence-rejected on a reconciliation dataset side (source_config or a target_configs[]
# entry) when the OWNING FLOW's execution_mode is "pipeline"/"pipeline_audit_only". Wired into
# _validate_reconciliation_dataset_config via reject_mode_incompatible_keys, which validates
# one side at a time and so has no flow-level context of its own -- execution_mode is threaded
# in by the caller (_validate_reconciliation_flows).
REMOVED_RECONCILIATION_DATASET_KEYS_PIPELINE = {
    "task_run_id_column": (
        "engine/run_context.py::resolve_pipeline_run_id has no stable per-update key -- "
        "pipelines.id is the PIPELINE id, constant across every update, and narrowing by it "
        "would match every row that pipeline has ever written, i.e. a silent no-op. Use "
        "filter_condition instead, or set execution_mode to 'job' to keep the standalone "
        "engine's per-run task_run_id narrowing."
    ),
}
# Presence-rejected on a reconciliation FLOW while execution_mode is (still) "job" -- both
# attach to a dataset a job task has no equivalent for: publish_schema names where a
# pipeline-hosted flow's own recon__<id>__<target>__classified/__metrics/__mismatch datasets
# are published, and dq_config attaches dlt expectations to the one-row __metrics dataset a
# job task never produces at all.
RECONCILIATION_FLOW_KEYS_REQUIRING_PIPELINE_MODE = {
    "publish_schema": (
        "publish_schema names the schema where this flow's recon__<reconciliation_id>__"
        "<target_id>__classified/__metrics/__mismatch datasets are published inside the "
        "hosting Lakeflow pipeline -- a 'job' execution_mode flow has no such datasets at all. "
        "Set execution_mode to 'pipeline' or 'pipeline_audit_only', or delete this key."
    ),
    "dq_config": (
        "dq_config attaches dlt expectations to the one-row __metrics dataset -- a 'job' "
        "execution_mode flow never produces that dataset, so there is nothing to attach an "
        "expectation to. Set execution_mode to 'pipeline' or 'pipeline_audit_only', or delete "
        "this key."
    ),
}
ALLOWED_ASN1_CODECS = {"ber", "der"}
ALLOWED_OBSERVABILITY_DESTINATION_TYPES = {"DATABRICKS_VOLUME", "OTLP_CONSUMER"}
# observability[].mode -- which of the two observability engines serves this destination.
# "triggered" is the bounded post-update export (notebooks/08_observability), "continuous" the
# always-on streaming export (notebooks/06_observability_streaming). The two have fundamentally
# different lifecycles (a downstream job task vs. a `continuous: true` pipeline), so there is no
# single entrypoint that switches between them: `mode` is what stops one destination being
# served -- and therefore double-exported -- by both.
ALLOWED_OBSERVABILITY_MODES = {"triggered", "continuous"}
ALLOWED_OBSERVABILITY_AUTH_TYPES = {"BEARER_TOKEN", "API_KEY", "BASIC_AUTH", "NONE"}
ALLOWED_OBSERVABILITY_COMPRESSION = {"GZIP", "gzip", "none", ""}

# Delta Liquid Clustering's own hard limit on clustering columns.
# Kept in sync with the identical constant in storage/table_properties.py -- see the v1.3.0
# contract, E07. The two modules cannot import from each other without creating an
# onboarding -> storage dependency that does not exist today, so duplicating one integer literal
# (with this cross-reference in both files) is the deliberate trade over inventing that coupling.
MAX_LIQUID_CLUSTERING_COLUMNS = 3

# 'env:<VAR_NAME>' or 'secret:<scope>:<key>' -- see observability/destination_dispatcher.py::
# resolve_credential. Deliberately a different shape from check_secret_ref's UC 3-level dict
# (this module's own auth_config values are short strings, not nested objects -- see
# docs/27_dlt_observability_onboarding_reference.md's note on why).
_CREDENTIAL_REF_PATTERN = re.compile(r"^(env:[A-Za-z_][A-Za-z0-9_]*|secret:[^:]+:[^:]+)$")

# Registry of supported pre-extraction decryption algorithms -- see
# `ingestion/readers.py::_apply_source_zip_handling`. Adding a new algorithm later means adding
# a new key here (and a new handler function) -- never a schema change.
#: ``source_zip_handling.pre_extraction_decryption.type``. ``pgp`` is key-based (asymmetric);
#: ``pgp_symmetric`` (v1.7.4) is passphrase-based, the shape ``gpg --symmetric`` produces.
#: They are mutually exclusive at the OpenPGP message level -- a key-encrypted message is not
#: passphrase-decryptable and vice versa -- so they are separate types rather than one type
#: with two optional secret shapes.
ALLOWED_PRE_EXTRACTION_DECRYPTION_TYPES = {"pgp", "pgp_symmetric"}

#: ``source_zip_handling.member_format`` (v1.7.4) -- the CONTAINER, orthogonal to any
#: decryption layer. ``zip`` is the pre-v1.7.4 default and only behaviour.
ALLOWED_SOURCE_MEMBER_FORMATS = {"gzip", "zip"}

# Data-standardization SQL is a column-expression allowlist, never a full statement. Any bare
# occurrence of these keywords (case-insensitive, word-boundary matched) is rejected outright --
# this is deliberately conservative (better to reject a legitimate edge case than to silently
# accept a disguised full statement).
_FORBIDDEN_STANDARDIZATION_KEYWORDS = re.compile(
    r"\b(SELECT|FROM|JOIN|UNION|WHERE|INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|CREATE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Generic type/shape checks. Every helper appends a fully-qualified, human-readable
# message to `errors` on failure and returns nothing -- callers just keep going so one
# onboarding attempt surfaces every problem at once.
# ---------------------------------------------------------------------------


def _type_name(value: Any) -> str:
    return type(value).__name__


def check_bool(value: Any, path: str, errors: List[str], required: bool = False) -> None:
    """Validate that `value` is a real Python bool -- catches the classic 'abc' / 'true' (string) mistake."""
    if value is None:
        if required:
            errors.append(f"{path}: is required but was missing")
        return
    if not isinstance(value, bool):
        errors.append(
            f"{path}: expected a boolean (true/false in JSON), got {value!r} ({_type_name(value)}). "
            f"Use the JSON literals `true`/`false`, not a quoted string."
        )


def check_string(
    value: Any,
    path: str,
    errors: List[str],
    required: bool = False,
    allowed_values: Optional[set] = None,
) -> None:
    """Validate that `value` is a non-empty string, optionally restricted to `allowed_values`."""
    if value is None or value == "":
        if required:
            errors.append(f"{path}: is required but was missing or empty")
        return
    if not isinstance(value, str):
        errors.append(f"{path}: expected a string, got {value!r} ({_type_name(value)})")
        return
    if allowed_values is not None and value not in allowed_values:
        errors.append(f"{path}: has invalid value {value!r} -- allowed values are {sorted(allowed_values)}")


def check_int(
    value: Any, path: str, errors: List[str], required: bool = False, minimum: Optional[int] = None
) -> None:
    """Validate that `value` is an int (booleans are rejected even though `bool` subclasses `int`)."""
    if value is None:
        if required:
            errors.append(f"{path}: is required but was missing")
        return
    if isinstance(value, bool) or not isinstance(value, int):
        errors.append(f"{path}: expected an integer, got {value!r} ({_type_name(value)})")
        return
    if minimum is not None and value < minimum:
        errors.append(f"{path}: must be >= {minimum}, got {value}")


def check_list_of_str(value: Any, path: str, errors: List[str], required: bool = False) -> None:
    """Validate that `value` is a JSON array of strings."""
    if value is None:
        if required:
            errors.append(f"{path}: is required but was missing")
        return
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        errors.append(f"{path}: expected a list of strings, got {value!r}")


def check_dict(value: Any, path: str, errors: List[str], required: bool = False) -> bool:
    """Validate that `value` is a JSON object. Returns True if it is (so callers can inspect it further)."""
    if value is None:
        if required:
            errors.append(f"{path}: is required but was missing")
        return False
    if not isinstance(value, dict):
        errors.append(f"{path}: expected an object ({{...}}), got {value!r} ({_type_name(value)})")
        return False
    return True


def check_dict_of_str(value: Any, path: str, errors: List[str], required: bool = False) -> bool:
    """Validate that `value` is a JSON object whose keys and values are all strings."""
    if value is None:
        if required:
            errors.append(f"{path}: is required but was missing")
        return False
    if not isinstance(value, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in value.items()
    ):
        errors.append(f"{path}: expected an object of string -> string, got {value!r}")
        return False
    return True


def check_secret_ref(value: Any, path: str, errors: List[str], required: bool = False) -> None:
    """Validate a Unity Catalog three-level secret reference: ``{secret_catalog, secret_schema, secret_key}``.

    See ``crypto/secrets.py::resolve_secret_value`` -- resolved via
    ``dbutils.secrets.get(catalog=, schema=, key=)``, never a classic workspace scope.
    """
    if not check_dict(value, path, errors, required=required):
        return
    check_string(value.get("secret_catalog"), f"{path}.secret_catalog", errors, required=True)
    check_string(value.get("secret_schema"), f"{path}.secret_schema", errors, required=True)
    check_string(value.get("secret_key"), f"{path}.secret_key", errors, required=True)


def check_credential_ref(value: Any, path: str, errors: List[str], required: bool = False) -> None:
    """Validate an ``observability`` credential reference: ``'env:<VAR_NAME>'`` or
    ``'secret:<scope>:<key>'`` -- never a literal secret value. See
    ``observability/destination_dispatcher.py::resolve_credential``.
    """
    if value is None:
        if required:
            errors.append(f"{path}: is required but was missing")
        return
    if not isinstance(value, str) or not _CREDENTIAL_REF_PATTERN.match(value):
        errors.append(
            f"{path}: expected 'env:<VAR_NAME>' or 'secret:<scope>:<key>', got {value!r} -- literal secret "
            "values are never allowed here"
        )


# ---------------------------------------------------------------------------
# Data-standardization SQL: a restricted, column-expression-only grammar.
# ---------------------------------------------------------------------------


def _validate_standardization_expression(expression: str, path: str, errors: List[str]) -> None:
    if _FORBIDDEN_STANDARDIZATION_KEYWORDS.search(expression):
        errors.append(
            f"{path}: {expression!r} is not a valid data-standardization expression -- only a single column "
            "expression is allowed (e.g. 'trim(customer_name) AS customer_name'), never a SELECT/FROM/JOIN/"
            "UNION or any other full SQL statement"
        )
        return
    if ";" in expression:
        errors.append(f"{path}: {expression!r} must not contain ';' -- exactly one column expression per entry")


def _validate_data_standardization_sql(expressions: Any, path_prefix: str, errors: List[str]) -> None:
    if expressions is None:
        return
    if not isinstance(expressions, list) or not all(isinstance(item, str) for item in expressions):
        errors.append(f"{path_prefix}: expected a list of column-expression strings, got {expressions!r}")
        return
    for index, expression in enumerate(expressions):
        if not expression.strip():
            errors.append(f"{path_prefix}[{index}]: must not be empty")
            continue
        _validate_standardization_expression(expression, f"{path_prefix}[{index}]", errors)


# ---------------------------------------------------------------------------
# Domain-specific sub-validators, shared between ingestion and transformation flows.
# ---------------------------------------------------------------------------


def _validate_dq_config(dq_config: Any, path_prefix: str, errors: List[str]) -> None:
    """Validate ``dq_config``: ``rules[]``, ``quarantine_table``, ``record_id_column``.

    ``quarantine_table`` naming a table here does not by itself create one -- the quarantine
    sibling table is only ever registered when at least one rule in ``rules`` has
    ``action == "quarantine"`` (see ``dq/quarantine.py::register_main_and_quarantine_tables``).
    A ``quarantine_table`` name with no quarantine-action rule is accepted (not an error) but
    produces no table -- flagged here only as a `logger.warning`-worthy no-op, not a hard error,
    since a spec author may legitimately be about to add such a rule.
    """
    if dq_config is None:
        return
    if not check_dict(dq_config, path_prefix, errors):
        return
    reject_unknown_keys(dq_config, path_prefix, errors, ALLOWED_DQ_CONFIG_KEYS)

    rules = dq_config.get("rules")
    if rules is not None:
        if not isinstance(rules, list):
            errors.append(f"{path_prefix}.rules: expected a list of DQ rule objects, got {rules!r}")
        else:
            for index, rule in enumerate(rules):
                rule_path = f"{path_prefix}.rules[{index}]"
                if not check_dict(rule, rule_path, errors):
                    continue
                check_string(rule.get("rule_id"), f"{rule_path}.rule_id", errors, required=True)
                check_string(rule.get("expression"), f"{rule_path}.expression", errors, required=True)
                check_string(
                    rule.get("action"), f"{rule_path}.action", errors, required=True, allowed_values=ALLOWED_DQ_ACTIONS
                )

    if dq_config.get("quarantine_table") is not None:
        check_string(dq_config.get("quarantine_table"), f"{path_prefix}.quarantine_table", errors)
    if dq_config.get("record_id_column") is not None:
        check_string(dq_config.get("record_id_column"), f"{path_prefix}.record_id_column", errors)


def _validate_governance_tags(governance_tags: Any, path_prefix: str, errors: List[str]) -> None:
    """Validate ``governance_tags``: key-value, multi-tag, both column- and table-level.

    Policy creation/administration (what a tag *does*, e.g. which UC row-filter/column-mask
    policy a given tag value activates) is explicitly out of framework scope -- the framework
    only applies the tags (see ``governance/tags.py``).
    """
    if governance_tags is None:
        return
    if not check_dict(governance_tags, path_prefix, errors):
        return
    reject_unknown_keys(governance_tags, path_prefix, errors, ALLOWED_GOVERNANCE_TAGS_KEYS)

    column_tags = governance_tags.get("column_tags")
    if column_tags is not None:
        if not isinstance(column_tags, list):
            errors.append(f"{path_prefix}.column_tags: expected a list, got {column_tags!r}")
        else:
            for index, entry in enumerate(column_tags):
                entry_path = f"{path_prefix}.column_tags[{index}]"
                if not check_dict(entry, entry_path, errors):
                    continue
                check_string(entry.get("column"), f"{entry_path}.column", errors, required=True)
                check_dict_of_str(entry.get("tags"), f"{entry_path}.tags", errors, required=True)

    if governance_tags.get("table_tags") is not None:
        check_dict_of_str(governance_tags.get("table_tags"), f"{path_prefix}.table_tags", errors)


def _validate_encrypted_columns(columns: Any, path_prefix: str, errors: List[str]) -> None:
    """Validate ``target_config.encrypted_columns`` -- output-column encryption only.

    ``source_data_type`` (added v1.4.0) is the one **optional declaration** here: the original
    source data type of the column being encrypted, e.g. ``"string"``, ``"decimal(18,2)"``,
    ``"timestamp"``. Encryption replaces a column's physical type with ciphertext binary, so
    without a record of what it was, a downstream ``decrypted_columns[].cast_to_type`` is an
    unverifiable guess.

    It stays optional, and is validated only as a non-empty string, for two reasons. First,
    omitting it is safe: ``crypto/column_crypto.py::apply_aes_column_encryption`` falls back to
    the type Spark actually reports for the column at encryption time, which is the behaviour
    every pre-v1.4.0 spec already had -- so this is purely additive and no existing spec needs
    editing. Second, the declared value is a *Spark* type string that this validator has no Spark
    session to parse and no source schema to check it against; the meaningful comparison is
    declared-vs-observed, and that comparison can only happen where both exist, which is at
    encryption time. Declaring it there is what turns a silent type drift at the source into a
    loud failure: if the source column quietly changes from ``string`` to ``int``, the declared
    value no longer matches what Spark reports and the flow says so, instead of tagging the new
    type and letting a downstream ``cast_to_type`` start failing for reasons nobody can trace.
    """
    if columns is None:
        return
    if not isinstance(columns, list):
        errors.append(f"{path_prefix}: expected a list of column configs, got {columns!r}")
        return
    for index, col_config in enumerate(columns):
        col_path = f"{path_prefix}[{index}]"
        if not check_dict(col_config, col_path, errors):
            continue
        check_string(col_config.get("column_name"), f"{col_path}.column_name", errors, required=True)
        if col_config.get("output_column") is not None:
            check_string(col_config.get("output_column"), f"{col_path}.output_column", errors)
        if col_config.get("mode") is not None:
            check_string(col_config.get("mode"), f"{col_path}.mode", errors, allowed_values=ALLOWED_AES_MODES)
        if col_config.get("source_data_type") is not None:
            check_string(col_config.get("source_data_type"), f"{col_path}.source_data_type", errors)
        check_secret_ref(col_config.get("secret"), f"{col_path}.secret", errors, required=True)


def _validate_decrypted_columns(columns: Any, path_prefix: str, errors: List[str]) -> None:
    """Validate ``source_inputs[].decrypted_columns`` -- the ONLY valid location for decryption.

    ``cast_to_type`` is required: decryption may change the physical column type, and the
    framework checks it against the encrypted column's tagged ``original_data_type`` before the
    pipeline update proceeds (see ``crypto/column_crypto.py``).
    """
    if columns is None:
        return
    if not isinstance(columns, list):
        errors.append(f"{path_prefix}: expected a list of column configs, got {columns!r}")
        return
    for index, col_config in enumerate(columns):
        col_path = f"{path_prefix}[{index}]"
        if not check_dict(col_config, col_path, errors):
            continue
        check_string(col_config.get("column_name"), f"{col_path}.column_name", errors, required=True)
        if col_config.get("output_column") is not None:
            check_string(col_config.get("output_column"), f"{col_path}.output_column", errors)
        check_string(col_config.get("cast_to_type"), f"{col_path}.cast_to_type", errors, required=True)
        check_secret_ref(col_config.get("secret"), f"{col_path}.secret", errors, required=True)


def _validate_auto_ttl(auto_ttl: Any, path_prefix: str, errors: List[str], cdc_load_strategy: Optional[str]) -> None:
    """Validate ``target_config.auto_ttl`` -- see ``storage/table_properties.py::build_auto_ttl_kwarg``.

    ``timestamp_column``/``expire_in_days`` are individually optional: Auto TTL is an opt-in
    feature, so supplying only one of the two sub-fields (or neither) is not a validation
    error -- it just means Auto TTL is not applied for this flow. A present-but-invalid value
    (e.g. ``expire_in_days: 0``) is still a hard error.
    """
    if auto_ttl is None:
        return
    if not check_dict(auto_ttl, path_prefix, errors):
        return
    check_string(auto_ttl.get("timestamp_column"), f"{path_prefix}.timestamp_column", errors)
    if auto_ttl.get("expire_in_days") is not None:
        check_int(auto_ttl.get("expire_in_days"), f"{path_prefix}.expire_in_days", errors, minimum=1)
    if auto_ttl.get("timestamp_column") and auto_ttl.get("expire_in_days") is not None:
        if cdc_load_strategy not in {"APPEND", "TRUNCATE_AND_LOAD"}:
            errors.append(
                f"{path_prefix}: only supported for cdc_load_strategy in ['APPEND', 'TRUNCATE_AND_LOAD'] "
                f"(the only strategies the engine threads the auto_ttl decorator kwarg through), but this flow uses "
                f"'{cdc_load_strategy}'"
            )


def _validate_table_properties(table_properties: Any, path_prefix: str, errors: List[str]) -> None:
    if table_properties is None:
        return
    if not check_dict(table_properties, path_prefix, errors):
        return
    for duration_field in ("log_retention_duration", "deleted_file_retention_duration"):
        if table_properties.get(duration_field) is not None:
            check_string(table_properties.get(duration_field), f"{path_prefix}.{duration_field}", errors)
    if table_properties.get("enable_iceberg_read_uniformity") is not None:
        check_bool(
            table_properties.get("enable_iceberg_read_uniformity"),
            f"{path_prefix}.enable_iceberg_read_uniformity",
            errors,
        )


def _validate_sink_config(sink_config: Any, path_prefix: str, errors: List[str], target_type: Optional[str]) -> None:
    """Validate ``target_config.sink_config`` -- required for ``target_type in {"sink", "external_sink"}``.

    See ``engine/sink_registration.py`` / ``archive/pgp_zip_sink.py``. Every ``format`` maps
    to a real, genuine Lakeflow ``dlt.create_sink`` (Phase 7 -- "sink nodes must not appear
    as persisted datasets", never an ordinary DAG table write): ``"delta"``/``"kafka"``
    dispatch straight to Lakeflow's own native sink formats; ``"pgp_zip"`` is this
    framework's Python custom Lakeflow sink (a real ``pyspark.sql.datasource.DataSource``,
    registered via ``spark.dataSource.register``) used for encrypted-archive egress.

    ``"kafka"`` has no filesystem ``path`` at all -- unlike ``"delta"``/``"pgp_zip"``, its
    required fields live under ``kafka_options`` (the same flat options a Spark Structured
    Streaming Kafka writer takes; at minimum ``kafka.bootstrap.servers``/``topic`` -- see
    https://learn.microsoft.com/en-us/azure/databricks/ldp/ldp-sinks). ``kafka_secret_options``
    is this framework's own addition (no prior Kafka-sink convention existed anywhere in this
    repo to match) for the rarer case where a connector option's literal value must embed a
    resolved secret (e.g. ``kafka.sasl.jaas.config``) with no Unity Catalog service-credential
    alternative -- prefer ``kafka_options["databricks.serviceCredential"]`` when possible.
    """
    if not check_dict(sink_config, path_prefix, errors, required=True):
        return

    sink_format = sink_config.get("format")
    check_string(sink_format, f"{path_prefix}.format", errors, required=True, allowed_values=ALLOWED_SINK_FORMATS)

    if sink_format == "kafka":
        kafka_options = sink_config.get("kafka_options")
        if check_dict_of_str(kafka_options, f"{path_prefix}.kafka_options", errors, required=True):
            if not kafka_options.get("kafka.bootstrap.servers"):
                errors.append(
                    f"{path_prefix}.kafka_options: must include 'kafka.bootstrap.servers' -- the same "
                    "option a Spark Structured Streaming Kafka writer requires"
                )
            if not kafka_options.get("topic"):
                errors.append(f"{path_prefix}.kafka_options: must include 'topic'")
        kafka_secret_options = sink_config.get("kafka_secret_options")
        if kafka_secret_options is not None and check_dict(kafka_secret_options, f"{path_prefix}.kafka_secret_options", errors):
            for option_key, secret_ref in kafka_secret_options.items():
                check_secret_ref(secret_ref, f"{path_prefix}.kafka_secret_options.{option_key}", errors, required=True)
    else:
        # "delta": the Delta table/directory path. "pgp_zip": the per-microbatch raw-row
        # staging directory (see archive/pgp_zip_sink.py) -- NOT the finished-archive
        # location, which is post_export_archive.output_zip_path below.
        check_string(sink_config.get("path"), f"{path_prefix}.path", errors, required=True)

    staged_file_format = sink_config.get("staged_file_format")
    if staged_file_format is not None:
        # v1.6.0 -- the staged per-partition file format inside a pgp_zip archive: "json"
        # (default when absent -- JSON-Lines, the only pre-v1.6.0 behaviour) or "csv"
        # (RFC-4180 with a header row). Presence-rejected for the native sink formats,
        # which have no framework staging step at all: silently accepting it there would
        # let a spec assert a file shape nothing ever produces.
        if sink_format == "pgp_zip":
            check_string(
                staged_file_format,
                f"{path_prefix}.staged_file_format",
                errors,
                allowed_values=ALLOWED_STAGED_FILE_FORMATS,
            )
        else:
            errors.append(
                f"{path_prefix}.staged_file_format: only meaningful for format 'pgp_zip' (this "
                f"framework's staging-then-archive sink); format {sink_format!r} is a native "
                "Lakeflow sink with no staging step. Remove the attribute."
            )

    export_trigger = sink_config.get("export_trigger")
    if export_trigger is not None:
        # v1.7.5 -- see ALLOWED_EXPORT_TRIGGERS. Presence-rejected for the native sink formats
        # for the same reason as staged_file_format: they have no framework-driven export step
        # for a trigger to schedule, so accepting it would let a spec assert a cadence nothing
        # honours.
        if sink_format == "pgp_zip":
            check_string(
                export_trigger,
                f"{path_prefix}.export_trigger",
                errors,
                allowed_values=ALLOWED_EXPORT_TRIGGERS,
            )
        else:
            errors.append(
                f"{path_prefix}.export_trigger: only meaningful for format 'pgp_zip' (this "
                f"framework's staging-then-archive sink); format {sink_format!r} is a native "
                "Lakeflow sink whose write cadence Lakeflow itself owns. Remove the attribute."
            )

    staged_file_options = sink_config.get("staged_file_options")
    if staged_file_options is not None:
        options_path = f"{path_prefix}.staged_file_options"
        if sink_format != "pgp_zip":
            errors.append(
                f"{options_path}: only meaningful for format 'pgp_zip' (this framework's "
                f"staging-then-archive sink); format {sink_format!r} is a native Lakeflow sink "
                "with no staging step. Remove the attribute."
            )
        elif check_dict(staged_file_options, options_path, errors):
            reject_unknown_keys(staged_file_options, options_path, errors, ALLOWED_STAGED_FILE_OPTIONS_KEYS)
            if sink_config.get("staged_file_format") != "csv":
                # A dialect is a CSV concept. JSON-Lines has no delimiter and no header, so
                # accepting these there would let a spec assert a file shape nothing produces.
                errors.append(
                    f"{options_path}: only meaningful when staged_file_format == 'csv' "
                    "(JSON-Lines staging has no delimiter or header row)."
                )
            delimiter = staged_file_options.get("delimiter")
            if delimiter is not None:
                check_string(delimiter, f"{options_path}.delimiter", errors)
                if isinstance(delimiter, str) and len(delimiter) != 1:
                    errors.append(
                        f"{options_path}.delimiter: must be exactly one character, got {delimiter!r} -- "
                        "Python's csv writer cannot emit a multi-character delimiter."
                    )
            if staged_file_options.get("include_header") is not None:
                check_bool(staged_file_options.get("include_header"), f"{options_path}.include_header", errors)
            if staged_file_options.get("line_terminator") is not None:
                check_string(
                    staged_file_options.get("line_terminator"),
                    f"{options_path}.line_terminator",
                    errors,
                    allowed_values=ALLOWED_STAGED_LINE_TERMINATORS,
                )

    archive_config = sink_config.get("post_export_archive")
    # post_export_archive is REQUIRED for "pgp_zip" -- archiving (optionally PGP-encrypting)
    # every microbatch's output is the entire point of that sink format; it's still accepted
    # (structurally validated, but unused by engine/sink_registration.py) for "delta"/"kafka"
    # only if a spec author supplies it, since neither native sink format has any concept of
    # a post-write archiving step to hook it up to.
    archive_required = sink_format == "pgp_zip"
    if archive_required or archive_config is not None:
        if check_dict(archive_config, f"{path_prefix}.post_export_archive", errors, required=archive_required):
            check_bool(archive_config.get("enabled"), f"{path_prefix}.post_export_archive.enabled", errors, required=True)
            if archive_required and archive_config.get("enabled") is False:
                errors.append(
                    f"{path_prefix}.post_export_archive.enabled: must be true for format 'pgp_zip' -- "
                    "archiving IS what this sink format does; use format 'delta' or 'kafka' instead for "
                    "a sink with no archiving step"
                )
            if archive_config.get("enabled"):
                check_string(
                    archive_config.get("output_zip_path"), f"{path_prefix}.post_export_archive.output_zip_path", errors, required=True
                )
                if archive_config.get("export_file_name_format") is not None:
                    # A str.format()-style template for the exported archive's own file name
                    # (placeholders {batch_id}/{timestamp}) -- see
                    # archive/pgp_zip_sink.py::_PgpZipStreamWriter._render_export_file_name.
                    check_string(
                        archive_config.get("export_file_name_format"),
                        f"{path_prefix}.post_export_archive.export_file_name_format",
                        errors,
                    )
                if archive_config.get("archive_format") is not None:
                    check_string(
                        archive_config.get("archive_format"),
                        f"{path_prefix}.post_export_archive.archive_format",
                        errors,
                        allowed_values=ALLOWED_ARCHIVE_FORMATS,
                    )
                if archive_config.get("secret") is not None:
                    # Optional AES password protection on the ZIP itself (archive/zip_utils.py),
                    # independent of -- and combinable with -- pgp_encryption below.
                    check_secret_ref(archive_config.get("secret"), f"{path_prefix}.post_export_archive.secret", errors)
                pgp_encryption = archive_config.get("pgp_encryption")
                if pgp_encryption is not None and check_dict(pgp_encryption, f"{path_prefix}.post_export_archive.pgp_encryption", errors):
                    pgp_path = f"{path_prefix}.post_export_archive.pgp_encryption"
                    check_bool(pgp_encryption.get("enabled"), f"{pgp_path}.enabled", errors, required=True)
                    if pgp_encryption.get("enabled"):
                        # v1.7.4: EXACTLY ONE of passphrase_secret (symmetric) or
                        # recipient_public_key_secret (asymmetric). A PGP message is encrypted
                        # either to a recipient key or under a shared passphrase, never both,
                        # and a spec naming both leaves which one applies ambiguous.
                        passphrase_secret = pgp_encryption.get("passphrase_secret")
                        recipient_secret = pgp_encryption.get("recipient_public_key_secret")
                        if passphrase_secret is not None and recipient_secret is not None:
                            errors.append(
                                f"{pgp_path}: 'passphrase_secret' (symmetric) and "
                                "'recipient_public_key_secret' (asymmetric) are mutually exclusive -- "
                                "set exactly one."
                            )
                        elif passphrase_secret is not None:
                            check_secret_ref(passphrase_secret, f"{pgp_path}.passphrase_secret", errors, required=True)
                            for asymmetric_only in ("sign_with_private_key_secret", "sign_passphrase_secret"):
                                if pgp_encryption.get(asymmetric_only) is not None:
                                    errors.append(
                                        f"{pgp_path}.{asymmetric_only}: signing requires a sender keypair and is "
                                        "not available for symmetric ('passphrase_secret') encryption."
                                    )
                        else:
                            check_secret_ref(recipient_secret, f"{pgp_path}.recipient_public_key_secret", errors, required=True)
                        # Signing is asymmetric-only, and the symmetric branch above has already
                        # rejected both signing keys with a specific message -- so skip rather
                        # than re-report them here as generic secret-ref errors. Deliberately a
                        # guard rather than an early `return`: this block is currently last in
                        # the function, and a `return` would silently skip anything appended
                        # after it.
                        if passphrase_secret is None:
                            if pgp_encryption.get("sign_with_private_key_secret") is not None:
                                check_secret_ref(
                                    pgp_encryption.get("sign_with_private_key_secret"), f"{pgp_path}.sign_with_private_key_secret", errors
                                )
                            # sign_passphrase_secret is only meaningful alongside a signing key --
                            # a real, properly-secured signing private key is routinely
                            # passphrase-protected. Optional even when sign_with_private_key_secret
                            # is set (an unprotected signing key is a valid configuration too).
                            if pgp_encryption.get("sign_passphrase_secret") is not None:
                                if not pgp_encryption.get("sign_with_private_key_secret"):
                                    errors.append(
                                        f"{pgp_path}.sign_passphrase_secret: only meaningful alongside "
                                        "sign_with_private_key_secret, but that field is not set"
                                    )
                                check_secret_ref(pgp_encryption.get("sign_passphrase_secret"), f"{pgp_path}.sign_passphrase_secret", errors)


def _validate_target_config(
    target_config: Any, path_prefix: str, errors: List[str], target_type: Optional[str]
) -> Optional[str]:
    """Validate ``target_config``, including the CDC settings that now live directly inside it.

    Returns the resolved ``cdc_load_strategy`` (or ``None``) so callers can build the row for
    the control table's dedicated top-level column without re-parsing.
    """
    if target_config is None:
        return None
    if not check_dict(target_config, path_prefix, errors):
        return None

    if target_config.get("storage_format") is not None:
        check_string(target_config.get("storage_format"), f"{path_prefix}.storage_format", errors, allowed_values=ALLOWED_STORAGE_FORMATS)
        if target_config.get("storage_format") == "iceberg" and target_type != "batch_table":
            errors.append(
                f"{path_prefix}.storage_format: 'iceberg' is only valid when target_type == 'batch_table', "
                f"but this flow's target_type is '{target_type}' -- use 'delta' (optionally with "
                "table_properties.enable_iceberg_read_uniformity for Iceberg read compatibility on a Delta table)"
            )
    if target_config.get("partition_columns") is not None:
        # An explicitly empty list is VALID and means "no partitioning" -- deliberately not a
        # `minItems: 1`/non-empty check. It is behaviourally identical to omitting the field
        # (storage/table_properties.py::build_partition_and_cluster_kwargs emits no
        # `partition_cols` kwarg at all for either), but diagnostically distinct: the runtime
        # logs an INFO for the explicitly-empty case so an operator can tell "nobody configured
        # partitioning" from "someone decided against it".
        check_list_of_str(target_config.get("partition_columns"), f"{path_prefix}.partition_columns", errors)
    if target_config.get("liquid_clustering_columns") is not None:
        liquid_clustering_columns = target_config.get("liquid_clustering_columns")
        check_list_of_str(liquid_clustering_columns, f"{path_prefix}.liquid_clustering_columns", errors)
        # Delta Liquid Clustering itself accepts at most MAX_LIQUID_CLUSTERING_COLUMNS columns,
        # so a longer list is guaranteed to fail at table-creation time. Catching it here turns
        # an opaque mid-pipeline-update failure into an onboarding-time error naming the fix;
        # storage/table_properties.py::build_partition_and_cluster_kwargs repeats the same guard
        # at runtime for a control-table row that was hand-edited past this validator.
        if isinstance(liquid_clustering_columns, list) and len(liquid_clustering_columns) > MAX_LIQUID_CLUSTERING_COLUMNS:
            errors.append(
                f"{path_prefix}.liquid_clustering_columns: at most {MAX_LIQUID_CLUSTERING_COLUMNS} columns are "
                f"supported by Delta Liquid Clustering, got {len(liquid_clustering_columns)} "
                f"({liquid_clustering_columns}) -- reduce the list to {MAX_LIQUID_CLUSTERING_COLUMNS} or fewer columns"
            )
    _validate_table_properties(target_config.get("table_properties"), f"{path_prefix}.table_properties", errors)

    cdc_load_strategy = target_config.get("cdc_load_strategy")
    # Checked BEFORE the allowed-values test so a removed strategy reports why it was removed and
    # what replaces it, instead of the generic "not one of [...]" list -- which tells an author
    # their strategy is unknown, not that it was deliberately withdrawn one release ago.
    if cdc_load_strategy in REMOVED_CDC_LOAD_STRATEGIES:
        errors.append(f"{path_prefix}.cdc_load_strategy: {REMOVED_CDC_LOAD_STRATEGIES[cdc_load_strategy]}")
    else:
        check_string(
            cdc_load_strategy,
            f"{path_prefix}.cdc_load_strategy",
            errors,
            required=True,
            allowed_values=ALLOWED_TRANSFORMATION_CDC_STRATEGIES,  # superset; ingestion-only rejection at the caller
        )

    _validate_auto_ttl(target_config.get("auto_ttl"), f"{path_prefix}.auto_ttl", errors, cdc_load_strategy)
    _validate_encrypted_columns(target_config.get("encrypted_columns"), f"{path_prefix}.encrypted_columns", errors)

    strategies_requiring_keys = {"SCD1", "SCD2", "SCD3", "FULL_SNAPSHOT_CDC"}
    if cdc_load_strategy in strategies_requiring_keys:
        check_list_of_str(target_config.get("primary_keys"), f"{path_prefix}.primary_keys", errors, required=True)
    # sequence_by_column is OPTIONAL for SCD1/SCD2/SCD3 -- when absent, cdc/scd.py falls back to
    # __framework_ingestion_timestamp_utc as the sequencer (dlt.apply_changes always needs one).
    if target_config.get("sequence_by_column") is not None:
        check_string(target_config.get("sequence_by_column"), f"{path_prefix}.sequence_by_column", errors)
    # columns_to_check: empty/absent means "compare ALL applicable columns"; populated means
    # "compare only these" -- comparison-only, never required.
    if target_config.get("columns_to_check") is not None:
        check_list_of_str(target_config.get("columns_to_check"), f"{path_prefix}.columns_to_check", errors)

    # columns_to_exclude is COMPARISON-ONLY now (never dropped from the target table -- see
    # cdc/comparison_columns.py::resolve_comparison_columns). Still scoped to strategies that
    # have a comparison concept at all.
    strategies_supporting_comparison_exclusion = {"SCD1", "SCD2", "SCD3"}
    if target_config.get("columns_to_exclude") is not None:
        check_list_of_str(target_config.get("columns_to_exclude"), f"{path_prefix}.columns_to_exclude", errors)
        if cdc_load_strategy not in strategies_supporting_comparison_exclusion:
            errors.append(
                f"{path_prefix}.columns_to_exclude: only meaningful for cdc_load_strategy in "
                f"{sorted(strategies_supporting_comparison_exclusion)} (comparison-column exclusion), "
                f"but this flow uses '{cdc_load_strategy}'"
            )

    # cdc_operation_column/cdc_operation_mapping: OPTIONAL regardless of primary_keys -- a
    # source can have a real primary key with no explicit delete-marker column at all.
    strategies_supporting_delete_marker = {"SCD1", "SCD2", "FULL_SNAPSHOT_CDC"}
    if target_config.get("cdc_operation_column") is not None or target_config.get("cdc_operation_mapping") is not None:
        if cdc_load_strategy not in strategies_supporting_delete_marker:
            errors.append(
                f"{path_prefix}.cdc_operation_column: only meaningful for cdc_load_strategy in "
                f"{sorted(strategies_supporting_delete_marker)}, but this flow uses '{cdc_load_strategy}'"
            )
        check_string(target_config.get("cdc_operation_column"), f"{path_prefix}.cdc_operation_column", errors, required=True)
        operation_mapping = target_config.get("cdc_operation_mapping")
        if check_dict(operation_mapping, f"{path_prefix}.cdc_operation_mapping", errors, required=True):
            check_list_of_str(
                operation_mapping.get("delete_values"),
                f"{path_prefix}.cdc_operation_mapping.delete_values",
                errors,
                required=True,
            )

    if target_config.get("generate_hash_columns") is not None:
        check_bool(target_config.get("generate_hash_columns"), f"{path_prefix}.generate_hash_columns", errors)

    # generate_surrogate_key / surrogate_key_columns / surrogate_key_exclude_columns are all gone
    # (v1.4.0) -- there is no __framework_surrogate_key to scope. Row identity is primary_keys and
    # nothing else; change detection is columns_to_check/columns_to_exclude.
    reject_removed_keys(target_config, path_prefix, errors, REMOVED_TARGET_CONFIG_KEYS)
    reject_unknown_keys(target_config, path_prefix, errors, ALLOWED_TARGET_CONFIG_KEYS)

    if target_config.get("empty_target_if_source_empty") is not None:
        # TRUNCATE_AND_LOAD-only: it gates whether a zero-record source is allowed to blank the
        # target (default false -- the previous contents are preserved and the run logs why).
        # Every other strategy either appends or diffs, so there is no truncation to guard and a
        # value here would be silently inert -- which is worse than an error, because the author
        # would believe they had configured something.
        check_bool(target_config.get("empty_target_if_source_empty"), f"{path_prefix}.empty_target_if_source_empty", errors)
        if cdc_load_strategy != "TRUNCATE_AND_LOAD":
            errors.append(
                f"{path_prefix}.empty_target_if_source_empty: only meaningful for cdc_load_strategy "
                f"'TRUNCATE_AND_LOAD', but this flow uses '{cdc_load_strategy}'"
            )

    if target_config.get("capture_technical_metadata") is not None:
        # Transformation-flow-only equivalent of source_config.capture_technical_metadata
        # (transformation flows have no source_config to host it) -- gates
        # __framework_ingestion_timestamp_utc, see ingestion/technical_metadata.py.
        check_bool(target_config.get("capture_technical_metadata"), f"{path_prefix}.capture_technical_metadata", errors)

    if target_type in ("sink", "external_sink"):
        _validate_sink_config(target_config.get("sink_config"), f"{path_prefix}.sink_config", errors, target_type)

    return cdc_load_strategy if isinstance(cdc_load_strategy, str) else None


def _validate_landing_retention_policy(policy: Any, path_prefix: str, errors: List[str]) -> None:
    """Validate ``source_config.landing_retention_policy`` -- Auto Loader landing-zone cleanup
    only (``cloudFiles.cleanSource``), see ``ingestion/readers.py::resolve_landing_retention_policy``.

    Three deliberately permissive rules, each of which used to be (or would naively be) stricter:

    - ``archive_path`` is **never required**, not even for ``clean_source == "archive"``. An
      empty/absent ``archive_path`` on an ``"archive"`` policy is a documented *degrade to off*
      -- the reader logs a WARNING and takes no retention action at all. Hard-erroring here would
      break the single configuration an operator reaches for to temporarily disable archiving
      without deleting the whole block (and would fail the pipeline update rather than the
      onboarding, since a control-table row can be edited past this validator anyway).
    - ``clean_source == "delete"`` deletes by age alone; ``archive_path`` is neither required nor
      read for it. A stray ``archive_path`` alongside ``"delete"`` is ignored, not flagged -- an
      ignored sibling field is not an error.
    - ``retention_days`` has minimum **0**, not 1. Zero is meaningful: it means "no age threshold
      at all", i.e. a file becomes eligible for archive/delete as soon as Auto Loader has
      committed it (``cloudFiles.cleanSource.retentionDuration = "0 days"``). Omitting the field
      means 7 days (``ingestion/readers.py::DEFAULT_LANDING_RETENTION_DAYS``), NOT zero.
    """
    if policy is None:
        return
    if not check_dict(policy, path_prefix, errors):
        return
    check_string(policy.get("clean_source"), f"{path_prefix}.clean_source", errors, allowed_values=ALLOWED_CLEAN_SOURCE_MODES)
    # Deliberately NOT check_string(..., required=True): an empty/absent archive_path with
    # clean_source == 'archive' is the documented degrade-to-off above, not an error. Only the
    # type is checked, so `"archive_path": ""` validates cleanly.
    if policy.get("archive_path") is not None and not isinstance(policy.get("archive_path"), str):
        errors.append(f"{path_prefix}.archive_path: expected a string, got {policy.get('archive_path')!r}")
    if policy.get("retention_days") is not None:
        check_int(policy.get("retention_days"), f"{path_prefix}.retention_days", errors, minimum=0)


def _validate_pre_extraction_decryption(config: Any, path_prefix: str, errors: List[str]) -> None:
    """Validate ``source_zip_handling.pre_extraction_decryption`` -- fully optional at every
    level. Absent entirely, or present as ``{}``, means the archive needs no pre-extraction
    handling at all (a plain, unencrypted, non-password-protected ZIP).

    Two independent, combinable concerns live here, both optional:

    - ``type`` (dispatches to a specific PGP-style whole-file decryption handler --
      ``crypto/pgp.py`` for ``"pgp"`` today -- run on the raw bytes *before* the ZIP is ever
      opened, since the ZIP itself is the encrypted payload). Omit ``type`` when the file
      isn't wrapped in an outer decryption layer at all. Adding a future algorithm means
      registering a new handler and adding its name to
      ``ALLOWED_PRE_EXTRACTION_DECRYPTION_TYPES``, never restructuring this schema.
    - ``secret_passphrase`` (the AES-256 password on the ZIP archive itself, resolved by
      ``pyzipper`` at extraction time -- independent of, and combinable with, ``type``: e.g.
      PGP-decrypt the outer envelope first, then extract the password-protected ZIP it
      contained). Omit it for a ZIP with no archive-level password.
    """
    if config is None:
        return
    if not check_dict(config, path_prefix, errors):
        return
    decryption_type = config.get("type")
    if decryption_type is not None:
        check_string(decryption_type, f"{path_prefix}.type", errors, allowed_values=ALLOWED_PRE_EXTRACTION_DECRYPTION_TYPES)
        if decryption_type == "pgp_symmetric":
            # The shared passphrase IS the key here, so it is REQUIRED -- unlike the "pgp"
            # branch below, where passphrase_secret merely unlocks a protected private key.
            check_secret_ref(config.get("passphrase_secret"), f"{path_prefix}.passphrase_secret", errors, required=True)
            if config.get("private_key_secret") is not None:
                errors.append(
                    f"{path_prefix}.private_key_secret: not valid for type 'pgp_symmetric' -- a "
                    "passphrase-encrypted OpenPGP message has no recipient keypair. Use type 'pgp' "
                    "for a key-encrypted message."
                )
        elif decryption_type == "pgp":
            check_secret_ref(config.get("private_key_secret"), f"{path_prefix}.private_key_secret", errors, required=True)
            # passphrase_secret is OPTIONAL -- a real, properly-secured PGP private key is
            # routinely passphrase-protected, unlike this project's own throwaway test keypairs.
            # When present, it must be a genuine secret ref (never an inline passphrase literal).
            if config.get("passphrase_secret") is not None:
                check_secret_ref(config.get("passphrase_secret"), f"{path_prefix}.passphrase_secret", errors)
    if config.get("secret_passphrase") is not None:
        check_secret_ref(config.get("secret_passphrase"), f"{path_prefix}.secret_passphrase", errors)


def _validate_delete_source_after_extract(value: Any, path_prefix: str, errors: List[str]) -> None:
    """Validate ``source_zip_handling.delete_source_after_extract`` -- a boolean OR an action object.

    Both spellings are first-class and neither is deprecated. The legacy plain boolean is what
    every pre-v1.3.0 spec in this repo uses, and it is normalized at runtime by
    ``archive/zip_utils.py::resolve_zip_delete_policy`` (`true` -> ``delete_now``, `false` ->
    the internal-only ``never``); the object form exists to express the one thing a boolean
    cannot -- ``{"action": "delete_after_x_days", "days": N}``, which leaves *this* run's own
    just-extracted archive alone and instead sweeps the landing directory for archives that have
    already aged past ``days``.

    ``days`` is required for ``delete_after_x_days`` (a sweep with no threshold is not a policy,
    it is ``delete_now`` spelled confusingly) and rejected for ``delete_now`` (where it would be
    silently inert, which reads as a working configuration but is not one). ``days: 0`` is legal
    and means "sweep everything already committed", mirroring ``retention_days: 0``.
    """
    if value is None:
        # Absent means today's default -- delete this run's own archive immediately after a
        # successful extract. Every existing spec depends on that default.
        return
    if isinstance(value, bool):
        return
    if not isinstance(value, dict):
        errors.append(
            f"{path_prefix}: expected a boolean (legacy form) or an object "
            '{"action": "delete_now"} / {"action": "delete_after_x_days", "days": <int>}, '
            f"got {value!r}"
        )
        return

    action = value.get("action")
    check_string(action, f"{path_prefix}.action", errors, required=True, allowed_values=ALLOWED_ZIP_DELETE_ACTIONS)
    if action == "delete_after_x_days":
        check_int(value.get("days"), f"{path_prefix}.days", errors, required=True, minimum=0)
    elif action == "delete_now" and value.get("days") is not None:
        errors.append(f"{path_prefix}.days: only meaningful for action 'delete_after_x_days', but action is 'delete_now'")


def _validate_source_zip_handling(zip_handling: Any, path_prefix: str, errors: List[str]) -> None:
    """Shared by ``autoloader`` and ``asn1`` -- see ``ingestion/readers.py::_apply_source_zip_handling``.

    ``source_zip_path`` is a landing *directory*, never a single file -- a real landing zone
    routinely accumulates more than one archive between pipeline updates. ``zip_file_pattern``
    (a glob, matched the same way ``file_pattern``/``pathGlobFilter`` selects
    ingested files, e.g. ``"orders_*.zip"`` or ``"*.zip"``) is therefore required whenever
    ``source_zip_handling`` is enabled, to select which archive(s) in that directory this
    pipeline update processes.

    Not applicable to ``zerobus``, which streams an existing Delta table rather than
    landing-zone files, so there is nothing to unzip.
    """
    if zip_handling is None:
        return
    if not check_dict(zip_handling, path_prefix, errors):
        return
    check_bool(zip_handling.get("enabled"), f"{path_prefix}.enabled", errors, required=True)
    if zip_handling.get("enabled"):
        check_string(zip_handling.get("source_zip_path"), f"{path_prefix}.source_zip_path", errors, required=True)
        check_string(zip_handling.get("zip_file_pattern"), f"{path_prefix}.zip_file_pattern", errors, required=True)
        check_string(zip_handling.get("target_volume_path"), f"{path_prefix}.target_volume_path", errors, required=True)
        _validate_delete_source_after_extract(
            zip_handling.get("delete_source_after_extract"), f"{path_prefix}.delete_source_after_extract", errors
        )
        _validate_pre_extraction_decryption(
            zip_handling.get("pre_extraction_decryption"), f"{path_prefix}.pre_extraction_decryption", errors
        )
        member_format = zip_handling.get("member_format")
        if member_format is not None:
            check_string(
                member_format, f"{path_prefix}.member_format", errors, allowed_values=ALLOWED_SOURCE_MEMBER_FORMATS
            )
            if member_format == "gzip":
                # A gzip stream has no archive password. Accepting one would let a spec assert
                # protection that nothing applies -- rejected here as well as at runtime so the
                # error arrives at onboarding, not mid-update.
                decryption = zip_handling.get("pre_extraction_decryption") or {}
                if isinstance(decryption, dict) and decryption.get("secret_passphrase") is not None:
                    errors.append(
                        f"{path_prefix}.pre_extraction_decryption.secret_passphrase: an AES password on a "
                        "ZIP archive, meaningless for member_format 'gzip' (a gzip stream has no password). "
                        "Use pre_extraction_decryption.type to decrypt an outer envelope."
                    )


def _validate_json_string_columns(value: Any, path_prefix: str, errors: List[str]) -> None:
    """Validate ``source_config.json_string_columns`` -- STRING columns holding a JSON document.

    Deliberately a separate field from ``explode_columns`` rather than an overload of it:
    ``explode_columns`` names columns that are *already* structs/arrays, and letting one field
    mean both "flatten this struct" and "first parse this string, then flatten it" would make the
    field's type-dependent behaviour invisible in the spec. Equally deliberately not named
    ``parquet_json_columns`` -- a JSON payload in a string column is just as common in CSV, Delta
    and Zerobus sources, and a Parquet-scoped name would be wrong the first time someone pointed
    it at a CSV feed.

    Two per-item shapes are accepted: the plain column-name string (or ``{"column": "..."}``),
    which falls back to Databricks' inferring ``from_json`` and therefore requires a *streaming*
    source with a checkpoint at runtime, and ``{"column": ..., "schema_ddl": ...}``, which is
    fully deterministic on batch and streaming alike and is the recommended form. Only the
    structure is checked here -- whether the named column exists, is string-typed, and (for the
    no-``schema_ddl`` shape) is being read from a streaming DataFrame are all runtime facts, so
    ``ingestion/json_flattening.py`` raises for those.

    A column named twice is reported: the second entry would silently overwrite the first's
    parse (both target the same output column), so one of the two ``schema_ddl`` values the
    author wrote would never be used.
    """
    if value is None:
        return
    if not isinstance(value, list):
        errors.append(
            f"{path_prefix}: expected a list of column names or "
            '{"column": ..., "schema_ddl": ...} objects, '
            f"got {value!r}"
        )
        return

    seen_columns: Dict[str, int] = {}
    for index, item in enumerate(value):
        column: Any = None
        if isinstance(item, str) and item:
            column = item
        elif isinstance(item, dict):
            check_string(item.get("column"), f"{path_prefix}[{index}].column", errors, required=True)
            check_string(item.get("schema_ddl"), f"{path_prefix}[{index}].schema_ddl", errors)
            column = item.get("column")
        else:
            errors.append(
                f"{path_prefix}[{index}]: expected a column name string or an object with a 'column' key, got {item!r}"
            )
            continue

        if isinstance(column, str) and column:
            if column in seen_columns:
                errors.append(f"{path_prefix}: column {column!r} is listed more than once")
            else:
                seen_columns[column] = index


def _validate_dedup_watermark(value: Any, path_prefix: str, errors: List[str], remove_dups: bool = False) -> None:
    """Validate ``source_config.dedup_watermark`` -- the bounded-state variant of ``remove_dups``.

    Without a watermark, ``dropDuplicates`` on a streaming DataFrame keeps **unbounded** state
    (every distinct row seen since the stream started, retained forever), which eventually
    degrades or fails a long-running pipeline. This block is how a flow opts into
    ``withWatermark(...).dropDuplicatesWithinWatermark(...)`` instead -- trading exactness (a
    late-arriving duplicate outside the watermark survives) for bounded memory.

    Both sub-fields are required together because a watermark is meaningless with only one of
    them, and the block is rejected outright without ``remove_dups: true``: on its own it
    configures nothing at all, so accepting it silently would let an author believe dedup was
    enabled when it was not.
    """
    if value is None:
        return
    if not check_dict(value, path_prefix, errors):
        return
    check_string(value.get("event_time_column"), f"{path_prefix}.event_time_column", errors, required=True)
    check_string(value.get("delay_threshold"), f"{path_prefix}.delay_threshold", errors, required=True)
    if not remove_dups:
        errors.append(
            f"{path_prefix}: only meaningful alongside remove_dups: true, but remove_dups is not enabled for this source"
        )


def _validate_column_normalization(value: Any, path_prefix: str, errors: List[str]) -> None:
    """Validate ``source_config.column_normalization`` -- the object form of column-name normalization.

    The ONLY switch as of v1.4.0 -- the legacy boolean ``normalize_column_names`` is removed and
    rejected (see ``REMOVED_SOURCE_CONFIG_KEYS``). Only ``case`` is configurable -- see
    ``ALLOWED_COLUMN_NORMALIZATION_CASES`` for why the character normalization and the collision
    check are both deliberately fixed.

    Both sub-fields stay individually optional: ``{"case": "upper"}`` alone is valid and means
    "normalization is off, and would upper-case if switched on" -- inert, not an error, the same
    forgiving stance ``auto_ttl`` takes. ``enabled`` now defaults to ``false`` in
    ``ingestion/column_normalization.py::resolve_column_normalization`` via a plain
    ``.get("enabled", False)``, because with the legacy boolean gone there is no second
    declaration for an absent ``enabled`` to defer to.
    """
    if value is None:
        return
    if not check_dict(value, path_prefix, errors):
        return
    if value.get("enabled") is not None:
        check_bool(value.get("enabled"), f"{path_prefix}.enabled", errors)
    if value.get("case") is not None:
        check_string(value.get("case"), f"{path_prefix}.case", errors, allowed_values=ALLOWED_COLUMN_NORMALIZATION_CASES)


def _validate_ingestion_source_config(
    source_type: Optional[str],
    source_config: Any,
    path_prefix: str,
    errors: List[str],
    target_catalog: Optional[str] = None,
    target_table: Optional[str] = None,
) -> None:
    if source_config is None:
        return
    if not check_dict(source_config, path_prefix, errors):
        return

    if source_config.get("capture_technical_metadata") is not None:
        check_bool(source_config.get("capture_technical_metadata"), f"{path_prefix}.capture_technical_metadata", errors)
    reject_removed_keys(source_config, path_prefix, errors, REMOVED_SOURCE_CONFIG_KEYS)
    reject_unknown_keys(source_config, path_prefix, errors, ALLOWED_INGESTION_SOURCE_CONFIG_KEYS)
    if source_config.get("column_normalization") is not None:
        _validate_column_normalization(
            source_config.get("column_normalization"), f"{path_prefix}.column_normalization", errors
        )
    if source_config.get("schema_config_path") is not None:
        # ingestion/schema_config.py -- external JSON/YAML schema file (type mapping,
        # nullability, comments, source-to-target renames). Only checked structurally here
        # (non-empty string) -- the referenced file's own shape is validated when it's
        # actually loaded at pipeline graph-definition time, not at onboarding time, since
        # onboarding has no Volume/workspace file access of its own to read it with.
        check_string(source_config.get("schema_config_path"), f"{path_prefix}.schema_config_path", errors, required=True)
    if source_config.get("schema_evolution_mode") is not None:
        check_string(
            source_config.get("schema_evolution_mode"),
            f"{path_prefix}.schema_evolution_mode",
            errors,
            allowed_values=ALLOWED_SCHEMA_EVOLUTION_MODES,
        )
    if source_config.get("file_pattern") is not None:
        check_string(source_config.get("file_pattern"), f"{path_prefix}.file_pattern", errors)
    if source_config.get("reader_options") is not None:
        check_dict_of_str(source_config.get("reader_options"), f"{path_prefix}.reader_options", errors)
    if source_config.get("explode_columns") is not None:
        # An explicitly EMPTY list is valid and load-bearing: present-but-empty means
        # "auto-flatten everything" (equivalent to auto_flatten_all: true), while ABSENT (or an
        # explicit null) stays a schema-preserving pass-through. That distinction is resolved at
        # the pipeline call site against the raw dict -- see
        # ingestion/json_flattening.py::resolve_auto_flatten_all -- because it cannot be made
        # after a .get(). Nothing to enforce here beyond the element type.
        check_list_of_str(source_config.get("explode_columns"), f"{path_prefix}.explode_columns", errors)
    if source_config.get("json_string_columns") is not None:
        _validate_json_string_columns(
            source_config.get("json_string_columns"), f"{path_prefix}.json_string_columns", errors
        )
    if source_config.get("auto_flatten_all") is not None:
        # ingestion/json_flattening.py -- opt-in "flatten everything" recursive struct-flatten
        # + array-explode pass, used only when explode_columns is empty/absent. Off by default
        # (an ABSENT explode_columns is a schema-preserving pass-through unless this is set).
        check_bool(source_config.get("auto_flatten_all"), f"{path_prefix}.auto_flatten_all", errors)
    if source_config.get("remove_dups") is not None:
        # ingestion/dedup.py -- full-row dropDuplicates over every column except the
        # __framework_* technical columns and _rescued_data/_metadata/_object_metadata/
        # _asn1_decode_error (including those would make every row unique and defeat the dedup
        # entirely). Runs after explode/auto-flatten and before data_standardization_sql.
        check_bool(source_config.get("remove_dups"), f"{path_prefix}.remove_dups", errors)
    if source_config.get("dedup_watermark") is not None:
        _validate_dedup_watermark(
            source_config.get("dedup_watermark"),
            f"{path_prefix}.dedup_watermark",
            errors,
            remove_dups=bool(source_config.get("remove_dups")),
        )
    _validate_data_standardization_sql(
        source_config.get("data_standardization_sql"), f"{path_prefix}.data_standardization_sql", errors
    )
    if source_config.get("landing_retention_policy") is not None and source_type not in ("autoloader", "asn1"):
        # landing_retention_policy maps to cloudFiles.cleanSource, which only exists on an Auto
        # Loader file read. A zerobus source streams an existing Delta table -- there is no
        # landing zone to clean, so the block would be silently inert. Note the invariant this
        # guard protects on the other side too: landing_retention_policy is NEVER applied to the
        # source_zip_handling pre-extraction path (that landing zone's cleanup is
        # delete_source_after_extract's job), so an autoloader flow's policy governs the
        # *extracted* files Auto Loader reads, never the raw archives.
        errors.append(
            f"{path_prefix}.landing_retention_policy: only applicable to source_type "
            f"'autoloader'/'asn1' (Auto Loader file ingestion), but this flow's source_type is '{source_type}'"
        )
    _validate_landing_retention_policy(source_config.get("landing_retention_policy"), f"{path_prefix}.landing_retention_policy", errors)

    if source_type in ("autoloader", "asn1") and not source_config.get("schema_location") and target_catalog and target_table:
        # Default to the same /Volumes/<catalog>/landing/_schemas/<table>/ convention every
        # example in this repo already uses. Auto Loader's cloudFiles.schemaLocation option
        # has no default of its own, but this framework does. Mutates source_config in place
        # so the derived value is what actually gets persisted to the control table.
        source_config["schema_location"] = f"/Volumes/{target_catalog}/landing/_schemas/{target_table}/"

    if source_type == "autoloader":
        check_string(source_config.get("path"), f"{path_prefix}.path", errors, required=True)
        check_string(source_config.get("format"), f"{path_prefix}.format", errors, required=True)
        check_string(source_config.get("schema_location"), f"{path_prefix}.schema_location", errors, required=True)
        _validate_source_zip_handling(source_config.get("source_zip_handling"), f"{path_prefix}.source_zip_handling", errors)
    elif source_type == "zerobus":
        check_string(source_config.get("source_catalog"), f"{path_prefix}.source_catalog", errors, required=True)
        check_string(source_config.get("source_schema"), f"{path_prefix}.source_schema", errors, required=True)
        check_string(source_config.get("source_table"), f"{path_prefix}.source_table", errors, required=True)
    elif source_type == "asn1":
        check_string(source_config.get("path"), f"{path_prefix}.path", errors, required=True)
        check_string(source_config.get("schema_location"), f"{path_prefix}.schema_location", errors, required=True)
        check_string(source_config.get("asn1_schema_path"), f"{path_prefix}.asn1_schema_path", errors, required=True)
        check_string(
            source_config.get("asn1_codec"), f"{path_prefix}.asn1_codec", errors, required=True, allowed_values=ALLOWED_ASN1_CODECS
        )
        # OPTIONAL since 0.0.2: an absent/blank asn1_pdu_name means "auto-detect the root PDU"
        # (asn1/decoder.py::detect_root_pdu_name walks the compiled module for the type nothing
        # else references). required=True here would make that feature unreachable from a spec --
        # the decoder supported it while onboarding still rejected the document. A value that IS
        # supplied is still type-checked, and remains an explicit override that wins over detection.
        if source_config.get("asn1_pdu_name") is not None:
            check_string(source_config.get("asn1_pdu_name"), f"{path_prefix}.asn1_pdu_name", errors, required=False)
        _validate_source_zip_handling(source_config.get("source_zip_handling"), f"{path_prefix}.source_zip_handling", errors)


def _validate_source_inputs(source_inputs: Any, path_prefix: str, errors: List[str]) -> None:
    if source_inputs is None:
        return
    if not isinstance(source_inputs, list):
        errors.append(f"{path_prefix}: expected a list of input objects, got {source_inputs!r}")
        return
    for index, input_config in enumerate(source_inputs):
        input_path = f"{path_prefix}[{index}]"
        if not check_dict(input_config, input_path, errors):
            continue
        reject_unknown_keys(input_config, input_path, errors, ALLOWED_SOURCE_INPUT_KEYS)
        check_string(input_config.get("input_name"), f"{input_path}.input_name", errors, required=True)
        check_string(input_config.get("table"), f"{input_path}.table", errors, required=True)
        if input_config.get("is_streaming") is not None:
            check_bool(input_config.get("is_streaming"), f"{input_path}.is_streaming", errors)
        watermark = input_config.get("watermark")
        if watermark is not None and check_dict(watermark, f"{input_path}.watermark", errors):
            check_string(watermark.get("event_time_column"), f"{input_path}.watermark.event_time_column", errors, required=True)
            check_string(watermark.get("delay_threshold"), f"{input_path}.watermark.delay_threshold", errors, required=True)
        _validate_decrypted_columns(input_config.get("decrypted_columns"), f"{input_path}.decrypted_columns", errors)


def _validate_no_duplicate_input_names(transformation_flows: List[Any], errors: List[str]) -> None:
    """Flag ``source_inputs[].input_name`` values reused across different transformation flows.

    Every transformation flow's ``source_inputs`` registers a ``@dlt.view`` in the *same*
    pipeline graph -- all flows in one onboarding spec share one ``dataflow_group_id``, and
    ``register_transformation_inputs`` registers each input under its literal ``input_name``
    with no per-flow namespacing.
    """
    seen_by: Dict[str, str] = {}
    for flow in transformation_flows:
        if not isinstance(flow, dict):
            continue
        flow_label = flow.get("flow_step_id", "<missing flow_step_id>")
        source_inputs = flow.get("source_inputs")
        if not isinstance(source_inputs, list):
            continue
        for input_config in source_inputs:
            if not isinstance(input_config, dict):
                continue
            input_name = input_config.get("input_name")
            if not isinstance(input_name, str) or not input_name:
                continue
            owner = seen_by.get(input_name)
            if owner is not None and owner != flow_label:
                errors.append(
                    f"transformation_flow[{flow_label}].source_inputs.input_name: '{input_name}' is already used by "
                    f"transformation_flow[{owner}] -- every transformation flow's source_inputs registers a view in "
                    "the same pipeline graph, so input_name must be unique across all transformation_flows in this "
                    "spec, not just within one flow"
                )
            else:
                seen_by[input_name] = flow_label


# ---------------------------------------------------------------------------
# Reconciliation flows: source_config + target_configs[] (multi-target), bidirectional per target.
# ---------------------------------------------------------------------------


def _validate_reconciliation_dataset_config(
    config: Any, path_prefix: str, errors: List[str], execution_mode: Optional[str] = None
) -> None:
    """Validate one reconciliation side (``source_config`` or a ``target_configs[]`` entry).

    As of v1.3.0 the only supported ``type`` is ``"table"`` -- see
    ``ALLOWED_RECON_DATASET_TYPES`` for why ``"file"``/``"sink"`` were dropped. The explicit
    scope error below is emitted *in addition to* the generic allowed-values message
    ``check_string`` produces, because the generic one ("allowed values are ['table']") tells an
    author what is legal but not what to do about the spec they already have; the scope error
    names the migration (read the file/sink output into a Delta table first).

    ``execution_mode`` is the OWNING FLOW's resolved ``execution_mode`` (``"job"`` when absent
    or invalid), threaded in by the caller since this function validates one side in isolation
    and has no flow-level context of its own. It is ``None`` only for a caller that has not
    (yet) resolved one -- treated the same as ``"job"``, the more restrictive default.
    """
    if not check_dict(config, path_prefix, errors, required=True):
        return
    pipeline_mode = execution_mode in _RECONCILIATION_PIPELINE_MODES

    dataset_type = config.get("type", "table")
    check_string(dataset_type, f"{path_prefix}.type", errors, allowed_values=ALLOWED_RECON_DATASET_TYPES)
    if dataset_type == "table":
        check_string(config.get("table"), f"{path_prefix}.table", errors, required=True)
    elif dataset_type is not None:
        # `"type": null` is left alone deliberately -- it round-trips as "unset", the same as an
        # absent key, and was never an error before.
        errors.append(
            f"{path_prefix}.type: reconciliation is supported for Delta tables only -- type must be 'table', "
            f"got {dataset_type!r}. Read the file/sink output into a Delta table first, then reconcile "
            "against that table."
        )

    if config.get("read_mode") is not None:
        check_string(config.get("read_mode"), f"{path_prefix}.read_mode", errors, allowed_values=ALLOWED_READ_MODES)
        if pipeline_mode and config.get("read_mode") == "streaming":
            errors.append(
                f"{path_prefix}.read_mode: 'streaming' is not supported when execution_mode is "
                f"{execution_mode!r}. The in-pipeline comparison is a whole-snapshot batch "
                "classification, and a stream-static join supports only inner/left_outer, which "
                "cannot express MISSING_IN_SOURCE. Use read_mode 'batch' (the default), or set "
                "execution_mode to 'job' to keep the standalone streaming engine."
            )
    if pipeline_mode:
        reject_mode_incompatible_keys(
            config, path_prefix, errors, REMOVED_RECONCILIATION_DATASET_KEYS_PIPELINE, execution_mode
        )
    if config.get("task_run_id_column") is not None:
        # The column on THIS side carrying the producing pipeline/job run id. When the task_run_id
        # job parameter is set, the side's read is narrowed to that run's rows (applied before
        # filter_condition, so a filter_condition can narrow it further). Reconciliation is
        # triggered-only as of v1.4.0, so this narrowing always applies when both are configured
        # -- there is no longer a continuous mode in which it is skipped. Absent means task_run_id
        # stays correlation-only, the pre-v1.3.0 behaviour --
        # which is why this is optional and unvalidated beyond its type: the framework's own
        # materialized tables carry __framework_pipeline_run_id, but a third-party table's
        # equivalent column can be named anything at all, so no value is privileged here.
        check_string(config.get("task_run_id_column"), f"{path_prefix}.task_run_id_column", errors)
    if config.get("filter_condition") is not None:
        check_string(config.get("filter_condition"), f"{path_prefix}.filter_condition", errors)
    _validate_data_standardization_sql(
        config.get("data_standardization_sql"), f"{path_prefix}.data_standardization_sql", errors
    )
    if config.get("hash_precomputed") is not None:
        check_bool(config.get("hash_precomputed"), f"{path_prefix}.hash_precomputed", errors)
        if config.get("hash_precomputed") and dataset_type != "table":
            errors.append(
                f"{path_prefix}.hash_precomputed: only valid when type == 'table' -- only a framework-managed "
                f"table can carry pre-built __framework_hash_key/__framework_hash_value columns, but this "
                f"dataset's type is '{dataset_type}'"
            )


def _validate_reconciliation_target_configs(
    target_configs: Any, path_prefix: str, errors: List[str], execution_mode: Optional[str] = None
) -> None:
    if not isinstance(target_configs, list) or not target_configs:
        errors.append(f"{path_prefix}: is required and must be a non-empty list of target objects, got {target_configs!r}")
        return

    seen_target_ids: Dict[str, int] = {}
    for index, target_config in enumerate(target_configs):
        target_path = f"{path_prefix}[{index}]"
        if not check_dict(target_config, target_path, errors):
            continue

        target_id = target_config.get("target_id")
        check_string(target_id, f"{target_path}.target_id", errors, required=True)
        if isinstance(target_id, str) and target_id:
            if target_id in seen_target_ids:
                errors.append(
                    f"{target_path}.target_id: '{target_id}' is already used by {path_prefix}[{seen_target_ids[target_id]}] "
                    "-- target_id must be unique within a reconciliation flow"
                )
            else:
                seen_target_ids[target_id] = index

        _validate_reconciliation_dataset_config(target_config, target_path, errors, execution_mode=execution_mode)

        if target_config.get("comparison_direction") is not None:
            check_string(
                target_config.get("comparison_direction"),
                f"{target_path}.comparison_direction",
                errors,
                allowed_values=ALLOWED_COMPARISON_DIRECTIONS,
            )
        comparison_direction = target_config.get("comparison_direction", "both")
        if comparison_direction in ("source_to_target", "both"):
            check_string(target_config.get("append_target_table"), f"{target_path}.append_target_table", errors, required=True)


def _validate_reconciliation_flows(
    spark: SparkSession, reconciliation_flows: Any, parameters: Dict[str, Any], errors: List[str]
) -> List[Dict[str, Any]]:
    if reconciliation_flows is None:
        return []
    if not isinstance(reconciliation_flows, list):
        errors.append(f"reconciliation_flows: expected a list, got {reconciliation_flows!r}")
        return []

    for flow in reconciliation_flows:
        recon_id = flow.get("reconciliation_id", "<missing reconciliation_id>") if isinstance(flow, dict) else "<not an object>"
        label = f"reconciliation_flow[{recon_id}]"
        if not check_dict(flow, label, errors, required=True):
            continue

        check_string(flow.get("reconciliation_id"), f"{label}.reconciliation_id", errors, required=True)
        # recon_mode and generate_surrogate_key are both gone (v1.4.0) -- reconciliation is
        # triggered-only, and there is no surrogate key to generate.
        reject_removed_keys(flow, label, errors, REMOVED_RECONCILIATION_FLOW_KEYS)
        reject_unknown_keys(flow, label, errors, ALLOWED_RECONCILIATION_FLOW_KEYS)
        if flow.get("two_tier_verification") is not None:
            # Default true. Phase 1 is a cheap per-side (row_count, bit_xor of
            # __framework_hash_key, bit_xor of __framework_hash_value) fingerprint that
            # short-circuits the whole comparison when both sides agree; Phase 2 (the full
            # hash-key join and column-level discrepancy mapping) runs only when Phase 1 reports
            # a difference. The opt-out exists because the XOR fold cancels in pairs, so Phase 1
            # can produce a false "equal" for a same-cardinality, even-multiplicity difference --
            # it can never produce a false "different", which is why it is only ever used as an
            # early-out. See reconciliation/matcher.py.
            check_bool(flow.get("two_tier_verification"), f"{label}.two_tier_verification", errors)

        # execution_mode -- "job" (default, unchanged standalone engine), "pipeline" (L3+L4 +
        # the L5 heal lane inside this flow's own dataflow_group_id's Lakeflow pipeline update),
        # or "pipeline_audit_only" (L3+L4 only; healing stays on the standalone job engine).
        # Checked BEFORE the allowed-values test resolves it, so an invalid value still falls
        # back to the more restrictive "job" reading below rather than silently skipping every
        # mode-conditional check that follows.
        raw_execution_mode = flow.get("execution_mode")
        if raw_execution_mode is not None:
            check_string(
                raw_execution_mode, f"{label}.execution_mode", errors, allowed_values=ALLOWED_RECONCILIATION_EXECUTION_MODES
            )
        execution_mode = raw_execution_mode if raw_execution_mode in ALLOWED_RECONCILIATION_EXECUTION_MODES else "job"
        pipeline_mode = execution_mode in _RECONCILIATION_PIPELINE_MODES

        if flow.get("dataflow_group_id") is not None:
            check_string(flow.get("dataflow_group_id"), f"{label}.dataflow_group_id", errors)
        if flow.get("publish_schema") is not None:
            # Only meaningful when this flow's recon__*/classified/metrics/mismatch datasets are
            # real, externally visible UC tables -- i.e. execution_mode != "job". Structural type
            # check always runs; the mode-incompatibility rejection below is additive to it, same
            # as every other "only meaningful when X" field in this file (e.g.
            # empty_target_if_source_empty).
            check_string(flow.get("publish_schema"), f"{label}.publish_schema", errors)

        dq_config = flow.get("dq_config")
        if dq_config is not None:
            _validate_dq_config(dq_config, f"{label}.dq_config", errors)
            if isinstance(dq_config, dict) and isinstance(dq_config.get("rules"), list):
                for rule_index, rule in enumerate(dq_config["rules"]):
                    if isinstance(rule, dict) and rule.get("action") == "quarantine":
                        errors.append(
                            f"{label}.dq_config.rules[{rule_index}].action: 'quarantine' is not supported for a "
                            "reconciliation flow -- there is nothing to quarantine on the one-row __metrics "
                            "dataset these rules attach to. Use 'warn'/'drop'/'fail' instead."
                        )

        if not pipeline_mode:
            reject_mode_incompatible_keys(
                flow, label, errors, RECONCILIATION_FLOW_KEYS_REQUIRING_PIPELINE_MODE, execution_mode
            )

        _validate_reconciliation_dataset_config(
            flow.get("source_config"), f"{label}.source_config", errors, execution_mode=execution_mode
        )
        _validate_path_parameters(flow.get("source_config"), parameters, f"{label}.source_config", errors)
        _validate_reconciliation_target_configs(
            flow.get("target_configs"), f"{label}.target_configs", errors, execution_mode=execution_mode
        )
        _validate_path_parameters(flow.get("target_configs"), parameters, f"{label}.target_configs", errors)
        check_list_of_str(flow.get("match_keys"), f"{label}.match_keys", errors, required=True)
        if flow.get("compare_columns") is not None:
            check_list_of_str(flow.get("compare_columns"), f"{label}.compare_columns", errors)
        if flow.get("transform_sql") is not None:
            check_string(flow.get("transform_sql"), f"{label}.transform_sql", errors)
            # transform_sql legitimately needs full SELECT/FROM (it reshapes a miss set to
            # match a target's schema -- see reconciliation/appender.py::apply_transform_sql),
            # unlike data_standardization_sql's restricted single-column-expression grammar --
            # so, like transformation_sql, it is only validated for parse-ability via the same
            # EXPLAIN-based check, not a keyword allowlist.
            if isinstance(flow.get("transform_sql"), str) and flow.get("transform_sql"):
                _validate_sql_syntax(spark, flow["transform_sql"], label, parameters, errors, field_name="transform_sql")

        error_handling = flow.get("error_handling")
        if error_handling is not None and check_dict(error_handling, f"{label}.error_handling", errors):
            check_string(
                error_handling.get("on_failure"),
                f"{label}.error_handling.on_failure",
                errors,
                allowed_values=ALLOWED_RECONCILIATION_FAILURE_MODES,
            )

        _validate_logging_config(
            flow.get("logging_config"),
            f"{label}.logging_config",
            errors,
            execution_mode=execution_mode,
            dq_config=dq_config,
        )

    return reconciliation_flows


def _validate_logging_config(
    logging_config: Any,
    label: str,
    errors: List[str],
    execution_mode: str = "job",
    dq_config: Any = None,
) -> None:
    """Validate the optional ``run_log_capture``/``mismatch_log_capture`` gates a reconciliation
    flow can set to skip its log writes for a high-frequency continuous flow. Both default to
    ``true`` -- absent is valid and changes nothing.

    **v1.6.0 contract:** ``run_log_capture`` gates ``reconciliation_run_log`` AND
    ``reconciliation_result`` (previously unconditional), and in pipeline mode whether the
    ``recon__*__metrics`` dataset is registered at all; ``mismatch_log_capture`` gates
    ``reconciliation_mismatch_log`` and the ``recon__*__mismatch`` dataset. Both false ==
    reconciliation persists ONLY to its business targets. Two combinations are rejected here
    (mirroring the graph-time guards in ``reconciliation/graph_registration.py``, so the
    contradiction surfaces at onboarding instead of on the first pipeline update):

    * ``dq_config.rules`` present while ``run_log_capture`` is ``false`` -- the flow's
      expectations attach to its ``__metrics`` dataset, which would not exist.
    * ``execution_mode: pipeline_audit_only`` with BOTH flags ``false`` -- the audit-only mode
      exists solely to produce the metrics/mismatch datasets and their control-table exports,
      so this combination registers compute with no output at all.

    This block is the **onboarded per-flow layer**. It is overridden at run time by the
    ``recon_run_log_capture``/``recon_mismatch_log`` job parameters (see
    ``notebooks/05_reconciliation/05_reconciliation_engine.py`` and
    ``reconciliation/appender.py::resolve_log_capture_flags``), which are tri-state -- unset
    defers to exactly this config. The two naming schemes are deliberately distinct rather than
    unified: an operator firefighting a runaway flow needs to silence log writes for one run
    without re-onboarding, and needs to be able to tell at a glance whether a value came from
    metadata or from the run they just launched."""
    # NOTE: an ABSENT logging_config is not an early exit. Since v1.7.3 both flags default to
    # FALSE, so omitting the block entirely is a real statement ("stay silent") that can itself
    # contradict dq_config.rules or execution_mode='pipeline_audit_only'. Returning here is what
    # let those two specs pass onboarding and then die at graph-definition time.
    if logging_config is None:
        logging_config = {}
    if not check_dict(logging_config, label, errors):
        return
    if logging_config.get("run_log_capture") is not None:
        check_bool(logging_config.get("run_log_capture"), f"{label}.run_log_capture", errors)
    if logging_config.get("mismatch_log_capture") is not None:
        check_bool(logging_config.get("mismatch_log_capture"), f"{label}.mismatch_log_capture", errors)

    # Presence-aware cross-field rules -- evaluated on the DEFAULTED values. Since v1.7.3 the
    # default is FALSE (absent == silent), matching resolve_log_capture_flags' layer 3 with no
    # job-parameter layer at onboarding time. These MUST agree: hardcoding `True` here while the
    # runtime resolves `False` is what made a spec that omits logging_config pass onboarding and
    # then raise at graph-definition time. A runtime pipeline-conf override producing the same
    # contradiction is caught again by the graph-time guards in reconciliation/graph_registration.py.
    run_log_capture = logging_config.get("run_log_capture", _DEFAULT_LOG_CAPTURE)
    mismatch_log_capture = logging_config.get("mismatch_log_capture", _DEFAULT_LOG_CAPTURE)
    dq_rules = dq_config.get("rules") if isinstance(dq_config, dict) else None
    if dq_rules and run_log_capture is False:
        errors.append(
            f"{label}.run_log_capture resolves to false, which is incompatible with "
            "dq_config.rules -- the flow's expectations attach to its recon__*__metrics dataset, "
            "which is only registered when run_log_capture is true. NOTE: since v1.7.3 both "
            "log-capture flags default to FALSE (reconciliation is silent by default), so this "
            "fires even when the spec never wrote 'false' -- omitting logging_config is enough. "
            "Set logging_config.run_log_capture: true explicitly, or remove the dq_config rules."
        )
    if (
        execution_mode == "pipeline_audit_only"
        and run_log_capture is False
        and mismatch_log_capture is False
    ):
        errors.append(
            f"{label}: execution_mode 'pipeline_audit_only' with both run_log_capture and "
            "mismatch_log_capture resolving to false registers compute with no output at all -- "
            "the audit-only mode exists solely to produce the metrics/mismatch datasets and their "
            "control-table exports. NOTE: since v1.7.3 both flags default to FALSE "
            "(reconciliation is silent by default), so this fires even when the spec never wrote "
            "'false' -- omitting logging_config is enough. Set logging_config.run_log_capture: "
            "true (and/or mismatch_log_capture: true) explicitly, or use execution_mode "
            "'job'/'pipeline'."
        )


def _validate_observability_auth(auth: Any, path_prefix: str, errors: List[str]) -> None:
    if auth is None:
        return
    if not check_dict(auth, path_prefix, errors):
        return

    auth_type = auth.get("type")
    check_string(auth_type, f"{path_prefix}.type", errors, required=True, allowed_values=ALLOWED_OBSERVABILITY_AUTH_TYPES)
    credentials = auth.get("credentials")
    credentials_path = f"{path_prefix}.credentials"

    if auth_type == "BEARER_TOKEN":
        if check_dict(credentials, credentials_path, errors, required=True):
            check_credential_ref(credentials.get("token"), f"{credentials_path}.token", errors, required=True)
    elif auth_type == "API_KEY":
        if check_dict(credentials, credentials_path, errors, required=True):
            check_string(credentials.get("header_name"), f"{credentials_path}.header_name", errors, required=True)
            check_credential_ref(credentials.get("api_key"), f"{credentials_path}.api_key", errors, required=True)
    elif auth_type == "BASIC_AUTH":
        if check_dict(credentials, credentials_path, errors, required=True):
            check_credential_ref(credentials.get("username"), f"{credentials_path}.username", errors, required=True)
            check_credential_ref(credentials.get("password"), f"{credentials_path}.password", errors, required=True)


def _validate_observability_retry(retry: Any, path_prefix: str, errors: List[str]) -> None:
    if retry is None:
        return
    if not check_dict(retry, path_prefix, errors):
        return
    check_int(retry.get("max_attempts"), f"{path_prefix}.max_attempts", errors, minimum=1)
    backoff_multiplier = retry.get("backoff_multiplier")
    if backoff_multiplier is not None and (isinstance(backoff_multiplier, bool) or not isinstance(backoff_multiplier, (int, float)) or backoff_multiplier <= 1):
        errors.append(f"{path_prefix}.backoff_multiplier: must be a number > 1, got {backoff_multiplier!r}")


def _validate_observability_destination_config(destination_type: Any, config: Any, path_prefix: str, errors: List[str]) -> None:
    if not check_dict(config, path_prefix, errors, required=True):
        return

    if destination_type == "DATABRICKS_VOLUME":
        volume_path = config.get("volume_path")
        check_string(volume_path, f"{path_prefix}.volume_path", errors, required=True)
        if isinstance(volume_path, str) and volume_path and not volume_path.startswith("/Volumes/"):
            errors.append(f"{path_prefix}.volume_path: must start with '/Volumes/', got {volume_path!r}")
        if config.get("file_format") is not None:
            check_string(config.get("file_format"), f"{path_prefix}.file_format", errors, allowed_values={"JSONL", "JSON"})
    elif destination_type == "OTLP_CONSUMER":
        endpoint = config.get("endpoint")
        check_string(endpoint, f"{path_prefix}.endpoint", errors, required=True)
        if isinstance(endpoint, str) and endpoint and not endpoint.startswith(("http://", "https://")):
            errors.append(f"{path_prefix}.endpoint: must be a full http:// or https:// URL, got {endpoint!r}")
        if config.get("protocol") is not None:
            check_string(
                config.get("protocol"), f"{path_prefix}.protocol", errors,
                allowed_values={"OTLP_HTTP_JSON", "OTLP_HTTP_PROTO", "OTLP_GRPC"},
            )
        if config.get("resource_attributes") is not None:
            check_dict_of_str(config.get("resource_attributes"), f"{path_prefix}.resource_attributes", errors)

    if config.get("compression") is not None:
        check_string(config.get("compression"), f"{path_prefix}.compression", errors, allowed_values=ALLOWED_OBSERVABILITY_COMPRESSION)


def _validate_observability_event_log_tables(destination: Dict[str, Any], path_prefix: str, errors: List[str]) -> None:
    """Validate ``destination_config.event_log_tables`` against the destination's own ``mode``.

    Reported against the *destination* path (``observability[i].destination_config....``) rather
    than from inside :func:`_validate_observability_destination_config`, because the rule is not
    a property of the destination_config shape at all -- it is a cross-field rule between ``mode``
    and ``destination_config``, and both are only in scope here.

    ``event_log_tables`` is required for, and only for, ``mode == "continuous"``. The continuous
    pipeline has no upstream task to resolve a pipeline id from -- it is a standing
    ``continuous: true`` pipeline, not a bounded post-update job task -- so the only way it can
    know what to stream is to be told. Conversely the triggered engine resolves its pipeline from
    the upstream task run, so a table list there would be silently ignored, which is why it is
    reported rather than tolerated.
    """
    mode = destination.get("mode") or "triggered"
    destination_config = destination.get("destination_config")
    event_log_tables = destination_config.get("event_log_tables") if isinstance(destination_config, dict) else None

    if event_log_tables is None:
        if mode == "continuous":
            errors.append(
                f"{path_prefix}.destination_config.event_log_tables: is required when mode is 'continuous' -- "
                "the continuous observability pipeline has no upstream task to resolve a pipeline from, so it "
                "must be told which event-log tables to stream"
            )
        return

    if mode == "triggered":
        errors.append(
            f"{path_prefix}.destination_config.event_log_tables: only meaningful when mode is 'continuous', "
            "but this destination's mode is 'triggered' (the triggered engine resolves its pipeline from the "
            "upstream task run)"
        )

    if (
        not isinstance(event_log_tables, list)
        or not event_log_tables
        or not all(isinstance(entry, str) for entry in event_log_tables)
    ):
        errors.append(
            f"{path_prefix}.destination_config.event_log_tables: expected a non-empty list of fully-qualified "
            f"'catalog.schema.table' names, got {event_log_tables!r}"
        )
        return

    for index, entry in enumerate(event_log_tables):
        # Each entry names a real Unity Catalog Delta table one source pipeline publishes its own
        # event log to, so it must be three-part: the continuous pipeline reads it directly with
        # spark.readStream.table(...) and has no default catalog/schema of its own to fall back on.
        parts = entry.split(".")
        if len(parts) != 3 or not all(parts):
            errors.append(
                f"{path_prefix}.destination_config.event_log_tables[{index}]: expected a fully-qualified "
                f"'catalog.schema.table' name, got {entry!r}"
            )


def _validate_observability_destinations(destinations: Any, errors: List[str]) -> List[Dict[str, Any]]:
    """Validate the top-level ``observability[]`` array (telemetry destinations for the DLT
    observability engine -- see ``docs/25_dlt_observability_module.md``). Unlike every other
    top-level flow array, this one does **not** count toward the "at least one flow array must
    be non-empty" requirement -- observability with no ingestion/transformation/reconciliation
    flow to attach to is meaningless, and ``upsert_observability_config`` is only ever called
    alongside at least one real flow upsert (see ``02_onboarding_engine.py``).
    """
    if destinations is None:
        return []
    if not isinstance(destinations, list):
        errors.append(f"observability: expected a list, got {destinations!r}")
        return []

    seen_destination_ids: Dict[str, int] = {}
    for index, destination in enumerate(destinations):
        label = f"observability[{index}]"
        if not check_dict(destination, label, errors, required=True):
            continue

        destination_id = destination.get("id")
        check_string(destination_id, f"{label}.id", errors, required=True)
        if isinstance(destination_id, str) and destination_id:
            if destination_id in seen_destination_ids:
                errors.append(
                    f"{label}.id: '{destination_id}' is already used by observability[{seen_destination_ids[destination_id]}] "
                    "-- destination id must be unique within a spec"
                )
            else:
                seen_destination_ids[destination_id] = index

        if destination.get("enabled") is not None:
            check_bool(destination.get("enabled"), f"{label}.enabled", errors)

        if destination.get("mode") is not None:
            # Nullable/absent resolves to "triggered" everywhere (observability/config_loader.py
            # reads it the same defensive way), so an observability_config row provisioned before
            # this field existed keeps working with no migration -- 01_setup only ever runs
            # CREATE TABLE IF NOT EXISTS.
            check_string(destination.get("mode"), f"{label}.mode", errors, allowed_values=ALLOWED_OBSERVABILITY_MODES)

        destination_type = destination.get("type")
        check_string(destination_type, f"{label}.type", errors, required=True, allowed_values=ALLOWED_OBSERVABILITY_DESTINATION_TYPES)
        _validate_observability_destination_config(destination_type, destination.get("destination_config"), f"{label}.destination_config", errors)
        _validate_observability_event_log_tables(destination, label, errors)
        _validate_observability_auth(destination.get("auth"), f"{label}.auth", errors)
        _validate_observability_retry(destination.get("retry"), f"{label}.retry", errors)
        if destination.get("timeout_ms") is not None:
            check_int(destination.get("timeout_ms"), f"{label}.timeout_ms", errors, minimum=1)

    return destinations


def _validate_sql_syntax(
    spark: SparkSession,
    sql_text: str,
    flow_label: str,
    parameters: Dict[str, Any],
    errors: List[str],
    field_name: str = "transformation_sql",
) -> None:
    """Best-effort SQL syntax/planning validation via ``EXPLAIN``, tolerant of unresolved table refs.

    Validates the *post-substitution* SQL (with every ``${param}`` resolved via
    :func:`transformation.parameters.substitute_dynamic_parameters`), not the raw spec text.
    A ``ParseException`` (raised directly by ``spark.sql(...)`` for malformed SQL grammar) is
    always a hard error.

    Everything past parsing is decided by inspecting ``EXPLAIN``'s own *returned* plan text,
    not by catching ``AnalysisException`` -- confirmed live, this workspace's Spark Connect/
    serverless environment never raises for a query-*planning* problem (unresolved reference,
    column-count mismatch, incompatible types, ...); ``EXPLAIN`` always returns a normal
    DataFrame, and a failed plan's text simply starts with a literal ``"Error occurred during
    query planning: "`` line instead of the usual ``"== Physical Plan =="``/``"== Parsed
    Logical Plan =="``. Within that text, only the specific, genuinely structural error codes
    in ``_STRUCTURAL_PLANNING_ERROR_CODES`` (``NUM_COLUMNS_MISMATCH``/``INCOMPATIBLE_COLUMN_TYPE``
    today -- e.g. a ``UNION``/``UNION ALL`` whose branches don't line up) are treated as hard
    onboarding-validation failures; every other "Error occurred during query planning" text --
    most commonly an unresolved table/column reference, since ``transformation_sql``
    legitimately references ``source_inputs[]`` view names that only exist once the pipeline
    actually runs, never at onboarding time -- is tolerated (logged, not reported as an error),
    the same as an ``AnalysisException`` was tolerated back when this environment actually
    raised one for that case. Supports both ``UNION`` and ``UNION ALL`` -- these parse and are
    now genuinely structurally validated (not just parsed) through this same ``EXPLAIN``-based
    path.

    ``field_name`` names the spec field being validated in reported errors -- defaults to
    ``"transformation_sql"`` (this function's original, still-primary caller); reconciliation
    flows pass ``"transform_sql"`` so their own errors point at the right field.
    """
    try:
        resolved_sql = substitute_dynamic_parameters(sql_text, parameters)
    except FrameworkConfigError as exc:
        errors.append(f"{flow_label}.{field_name}: {exc}")
        return

    if spark is None:
        # Offline callers (agent_tools.validate_json, unit tests, any pre-deployment lint that
        # has no cluster) pass spark=None deliberately. Parameter substitution above is the
        # Spark-free half of this check and has already run; EXPLAIN-based structural
        # validation needs a live session, so skip it rather than raising AttributeError and
        # reporting it as "unexpected error during validation" -- which would make every
        # transformation and reconciliation flow un-lintable offline.
        logger.debug(
            "%s.%s: no SparkSession -- parameter substitution checked, EXPLAIN-based structural "
            "validation skipped (runs at onboarding time on the cluster).",
            flow_label,
            field_name,
        )
        return

    try:
        from pyspark.errors import AnalysisException, ParseException
    except ImportError:  # pragma: no cover - older PySpark versions
        from pyspark.sql.utils import AnalysisException, ParseException

    try:
        explain_rows = spark.sql(f"EXPLAIN {resolved_sql}").collect()
    except ParseException as exc:
        errors.append(f"{flow_label}.{field_name}: failed to parse after parameter substitution -- {exc}")
        return
    except AnalysisException as exc:
        logger.warning(
            "%s.%s: parsed but could not be resolved yet (expected pre-deployment): %s",
            flow_label,
            field_name,
            exc,
        )
        return
    except Exception as exc:  # noqa: BLE001
        errors.append(f"{flow_label}.{field_name}: unexpected error during validation -- {exc}")
        return

    # Real bug found live (via a UNION ALL column-count-mismatch test spec): on this
    # workspace's Spark Connect/serverless environment, `EXPLAIN` does NOT raise ANY exception
    # for a query-planning problem -- not `ParseException`, not `AnalysisException` -- it
    # always returns a normal DataFrame; a failed plan's `plan` text just starts with a
    # literal "Error occurred during query planning: " line instead of the usual
    # "== Physical Plan ==" (or "== Parsed Logical Plan ==", for a plan Spark can't fully
    # resolve yet). The exception-only branches above never actually collected this
    # DataFrame, so this whole class of problem -- structural errors like
    # NUM_COLUMNS_MISMATCH/INCOMPATIBLE_COLUMN_TYPE on a UNION whose branches don't line up --
    # previously sailed through onboarding validation undetected and only failed once actually
    # deployed to a live pipeline.
    #
    # This must NOT be a blanket "any 'Error occurred' text is a hard failure", though: an
    # *unresolved reference* (e.g. `TABLE_OR_VIEW_NOT_FOUND`/`UNRESOLVED_COLUMN` -- normal and
    # expected here, since `transformation_sql` legitimately references `source_inputs[]` view
    # names that only exist once the pipeline actually runs, not at onboarding time) ALSO
    # surfaces as this exact same "Error occurred during query planning:" text on this
    # environment (confirmed live) rather than raising `AnalysisException` the way the
    # docstring above still correctly describes for other Spark environments. Blanket-failing
    # on any "Error occurred" text regressed that tolerance entirely (confirmed live: it broke
    # ordinary, valid `transformation_sql` referencing not-yet-deployed inputs). So only the
    # specific, genuinely structural error codes below -- which are wrong regardless of
    # deployment timing -- are treated as hard failures; every other "Error occurred during
    # query planning" text (including every unresolved-reference case) is tolerated exactly
    # like the old `AnalysisException` branch above already tolerated it.
    _STRUCTURAL_PLANNING_ERROR_CODES = ("NUM_COLUMNS_MISMATCH", "INCOMPATIBLE_COLUMN_TYPE")
    plan_text = "\n".join(row["plan"] for row in explain_rows if "plan" in row.asDict())
    if plan_text.startswith("Error occurred during query planning:") and any(
        code in plan_text for code in _STRUCTURAL_PLANNING_ERROR_CODES
    ):
        errors.append(
            f"{flow_label}.{field_name}: failed to plan after parameter substitution -- "
            f"{plan_text[len('Error occurred during query planning:'):].strip()}"
        )
    elif plan_text.startswith("Error occurred during query planning:"):
        logger.warning(
            "%s.%s: parsed but could not be resolved yet (expected pre-deployment): %s",
            flow_label,
            field_name,
            plan_text,
        )


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------


def _validate_source_plane(value: Any, path: str, errors: List[str]) -> None:
    """Validate the top-level ``source_plane`` block -- the L0 read-once plane's policy.

    NEW in v1.7.3. Until this release ``source_plane`` was listed in ``ALLOWED_ROOT_KEYS`` and
    then never looked at again: the block was accepted verbatim, so a typo'd key or an
    unrecognised ``materialize`` value onboarded cleanly, wrote its control-table row and
    silently fell back to the engine's default policy. ``onboarding_spec_full_reference.md``
    documented that gap in as many words; this function closes it.

    Two checks, deliberately different in kind:

    * ``materialize`` -- a VALUE check. The key itself remains legal; only the value ``"never"``
      is prohibited, under the Single-Read architectural mandate. It is tested BEFORE the
      allowed-values check so an author who wrote ``"never"`` is told it was deliberately
      withdrawn and what to do instead, rather than being handed the generic "not one of
      [always, auto]" list, which reads as "you made a typo". This is the
      ``REMOVED_CDC_LOAD_STRATEGIES`` pattern -- a removed value of a surviving key -- NOT the
      ``REMOVED_*_KEYS`` pattern, whose ``reject_removed_keys()`` fires on key PRESENCE and
      would therefore also reject the legal ``materialize: "always"``.
    * ``catalog``/``schema`` -- publication location for L0 nodes, both optional and nullable
      (v1.6.0's Intermediate Object Rule: absent means "do not publish", not "publish to the
      pipeline's own catalog"). Only their type is checked here; the engine treats them as
      meaningful only when BOTH are set.
    """
    if value is None:
        return
    if not check_dict(value, path, errors):
        return

    reject_unknown_keys(value, path, errors, ALLOWED_SOURCE_PLANE_KEYS)

    materialize = value.get("materialize")
    if materialize is not None:
        if materialize in REMOVED_MATERIALIZE_POLICIES:
            errors.append(f"{path}.materialize: {REMOVED_MATERIALIZE_POLICIES[materialize]}")
        else:
            check_string(
                materialize,
                f"{path}.materialize",
                errors,
                required=False,
                allowed_values=ALLOWED_MATERIALIZE_POLICIES,
            )

    for key in ("catalog", "schema"):
        if value.get(key) is not None:
            check_string(value.get(key), f"{path}.{key}", errors, required=False)


def _validate_spark_config(value: Any, path: str, errors: List[str]) -> None:
    """Validate the top-level ``spark_config`` object -- group-scoped Spark session configuration.

    Deliberately a sibling of ``pipeline_parameters``, never an extension of it:
    ``pipeline_parameters`` drives ``${param}`` **string substitution** into SQL and paths
    (``transformation/parameters.py``), while this block is applied with ``spark.conf.set`` before
    any flow is registered. Overloading one field with both meanings would make it impossible to
    tell, from the spec alone, whether a key was going to be substituted or set.

    Keys must start with ``spark.``. That is a real constraint, not decoration: this block is the
    middle layer of a three-layer precedence chain (``FRAMEWORK_SPARK_DEFAULTS`` < this <
    the pipeline resource's own ``dataflow.spark.conf``), and a key outside the ``spark.``
    namespace is far more likely to be a ``pipeline_parameters`` entry filed in the wrong block
    than a real configuration -- at runtime it would be accepted, fail to do anything observable,
    and be logged only as a WARNING.

    Values are restricted to string/number/boolean because every resolved value is rendered with
    ``str()`` (bools as lowercase ``true``/``false``) before ``spark.conf.set``; a nested object
    or array has no meaningful rendering, so it is rejected here rather than silently stringified
    into ``"{'a': 1}"``. See ``engine/spark_config.py``.
    """
    if value is None:
        return
    if not check_dict(value, path, errors):
        return
    for key, config_value in value.items():
        if not isinstance(key, str):
            errors.append(f"{path}: every key must be a Spark configuration name string, got key {key!r}")
            continue
        if not key.startswith("spark."):
            errors.append(
                f"{path}.{key}: expected a Spark configuration key (e.g. 'spark.sql.shuffle.partitions'), "
                f"got {key!r} -- keys must start with 'spark.'"
            )
        if not isinstance(config_value, (str, int, float, bool)):
            errors.append(
                f"{path}.{key}: expected a string, number, or boolean value, got {config_value!r} "
                f"({_type_name(config_value)})"
            )


def _validate_path_parameters(config: Any, parameters: Dict[str, Any], label: str, errors: List[str]) -> None:
    """Catch an undefined ``${param}`` reference in a source_config/target_config's path
    fields at onboarding time, instead of failing at pipeline run.

    Pure string check -- unlike :func:`_validate_sql_syntax`, needs no Spark session. Mirrors
    exactly what ``notebooks/03_engine/03_lakeflow_declarative_pipeline.py`` (and the
    reconciliation engine notebook) do at pipeline-run time: round-trip the config through
    ``json.dumps`` and resolve it via :func:`~transformation.parameters.substitute_path_parameters`.
    ``config`` is usually a ``dict`` (``source_config``/``target_config``) but reconciliation's
    ``target_configs`` is a ``list`` of them -- both round-trip through ``json.dumps`` fine.
    """
    if not isinstance(config, (dict, list)):
        return
    try:
        substitute_path_parameters(json.dumps(config), parameters)
    except FrameworkConfigError as exc:
        errors.append(f"{label}: {exc}")


# ---------------------------------------------------------------------------
# Reconciliation pipeline placement: cross-array checks that only make sense once every flow
# array in the spec is known (source of truth: context.read_once_contract's "THE DOUBLE-APPEND
# HAZARD"/"COLLISION HANDLING" sections). Every comparison below is a casefolded, fully-
# qualified ``catalog.schema.table`` string built through
# :func:`storage.table_properties.qualified_table_name` -- never a bare ``target_table`` --
# because ``flowx_testing/003_autoload_recon_append.json`` writes ``Excalibur_usecase``
# with a capital E, and a case-sensitive comparison would silently miss it.
# ---------------------------------------------------------------------------

# CDC strategies that dispatch through dlt.apply_changes[_from_snapshot] -- i.e. the producing
# target's Delta transaction log contains real MERGE/UPDATE/DELETE operations, not only
# appends. A reconciliation source resolving to one of these in a pipeline execution_mode would
# have its L5 heal lane appending into a comparison built over rows Lakeflow may still rewrite
# in place before the next update.
_RECONCILIATION_SOURCE_MERGE_CDC_STRATEGIES = {"SCD1", "SCD2", "SCD3", "FULL_SNAPSHOT_CDC"}


def _qualified_or_none(catalog: Any, schema: Any, table: Any) -> Optional[str]:
    """Best-effort casefolded ``catalog.schema.table`` out of three separate columns (an
    ingestion/transformation flow's own ``target_catalog``/``target_schema``/``target_table``,
    or a zerobus ingestion flow's ``source_catalog``/``source_schema``/``source_table``), or
    ``None`` if any part is missing or not a safe identifier.

    Used only for cross-flow *comparison* here, never persisted -- a spec that has not yet
    passed its own per-field ``check_string`` calls simply drops out of comparison instead of
    raising a second, redundant error about the same malformed value.
    """
    if not (isinstance(catalog, str) and isinstance(schema, str) and isinstance(table, str)):
        return None
    try:
        return qualified_table_name(catalog, schema, table).casefold()
    except ValueError:
        return None


def _qualified_table_ref_or_none(value: Any) -> Optional[str]:
    """Best-effort casefolded ``catalog.schema.table`` out of a bare dotted string, as
    reconciliation's own ``source_config.table``/``target_configs[].table``/
    ``append_target_table`` are all spelled -- distinct from :func:`_qualified_or_none`, which
    builds the same shape from three separate columns. Never a bare table name: only a
    genuine three-part reference is comparable at all.
    """
    if not isinstance(value, str) or not value:
        return None
    parts = value.split(".")
    if len(parts) != 3 or not all(parts):
        return None
    return _qualified_or_none(*parts)


def _validate_landing_side_effect_collisions(ingestion_flows: List[Any], errors: List[str]) -> None:
    """V-CYC-8: more than one distinct ``landing_retention_policy``/``source_zip_handling`` on
    one shared Auto Loader landing path is a latent DATA-LOSS bug even before source-plane
    sharing exists -- ``cloudFiles.cleanSource`` MOVES or DELETES committed landing files, and
    ``source_zip_handling`` PGP-decrypts, unzips and writes ``.__framework_extracted__``
    markers, so two competing lifecycle regimes on one directory corrupt whichever policy runs
    second. Compared as normalized (sorted-key) JSON so the whole nested shape is covered, not
    just a hand-picked subset of sub-fields.
    """
    seen_retention: Dict[str, Tuple[str, str]] = {}
    seen_zip: Dict[str, Tuple[str, str]] = {}
    for flow in ingestion_flows or []:
        if not isinstance(flow, dict):
            continue
        source_config = flow.get("source_config")
        if not isinstance(source_config, dict):
            continue
        path = source_config.get("path")
        if not isinstance(path, str) or not path:
            continue
        flow_label = str(flow.get("dataflow_id") or "<unknown ingestion flow>")

        retention_signature = json.dumps(source_config.get("landing_retention_policy") or {}, sort_keys=True)
        prior_retention = seen_retention.get(path)
        if prior_retention is None:
            seen_retention[path] = (retention_signature, flow_label)
        elif prior_retention[0] != retention_signature:
            errors.append(
                f"ingestion_flow[{flow_label}].source_config.landing_retention_policy: differs from "
                f"ingestion_flow[{prior_retention[1]}]'s, but both flows share landing path '{path}' -- "
                "cloudFiles.cleanSource MOVES or DELETES committed landing files, so two different "
                "retention policies on one directory is a data-loss race, not two independent settings"
            )

        zip_signature = json.dumps(source_config.get("source_zip_handling") or {}, sort_keys=True)
        prior_zip = seen_zip.get(path)
        if prior_zip is None:
            seen_zip[path] = (zip_signature, flow_label)
        elif prior_zip[0] != zip_signature:
            errors.append(
                f"ingestion_flow[{flow_label}].source_config.source_zip_handling: differs from "
                f"ingestion_flow[{prior_zip[1]}]'s, but both flows share landing path '{path}' -- "
                "source_zip_handling PGP-decrypts, unzips and writes .__framework_extracted__ markers, so "
                "two different zip-handling regimes on one directory corrupt whichever policy runs second"
            )


def _append_cycle_finding(pipeline_mode: bool, execution_mode: Any, message: str, errors: List[str]) -> None:
    """Record a V-CYC append-loop finding as a hard error in pipeline mode, a warning in job mode.

    Every one of these rules describes a *graph* cycle: a reconciliation flow appending into a
    dataset that the very same Lakeflow update also reads or writes. In ``execution_mode: "job"``
    there is no such graph -- the standalone ``05_reconciliation_engine.py`` task runs after the
    pipeline update has finished, so the append cannot race a read that is no longer happening.

    Making these unconditional errors was a real backward-compatibility break, not a stricter
    reading of an existing rule: ``flowx_testing/038_rec_003_precomputed_hash.json`` is a
    shipped, purely job-mode spec that appends into its own comparison target, and it stopped
    validating -- which means ``02_onboarding_engine.py`` (which raises on any non-empty
    ``errors``) could no longer onboard it, with no edit by its author and no opt-in to any
    v1.5.0 attribute.

    Job mode still gets the warning, because the underlying hazard is real there too (an append
    into a table Lakeflow owns can be clobbered by the next refresh). Downgrading it to a log
    line keeps that visible without rejecting a document that has always been accepted.
    """
    if pipeline_mode:
        errors.append(message)
        return
    logger.warning(
        "%s [reported as a warning, not an error, because execution_mode is %r: the standalone "
        "reconciliation job runs after the pipeline update, so there is no in-graph cycle. This "
        "WOULD be rejected under execution_mode 'pipeline'/'pipeline_audit_only'.]",
        message,
        execution_mode,
    )


def _validate_reconciliation_pipeline_placement(
    spec_group_id: Any,
    ingestion_flows: List[Any],
    transformation_flows: List[Any],
    reconciliation_flows: List[Any],
    errors: List[str],
) -> None:
    """Cross-array checks that only make sense once every flow array in the spec is known --
    modelled on :func:`_validate_no_duplicate_input_names`.

    A reconciliation flow's own ``dataflow_group_id`` (new in v1.5.0, independent of this
    spec's own top-level ``dataflow_group_id``) is what makes a flow's placement "this group"
    or "a different group" below: absent, it defaults to this spec's own group -- the
    overwhelmingly common case, a recon flow embedded in the same pipeline as the ingestion/
    transformation flows it was onboarded alongside. Declaring a *different* group is how a
    recon flow whose comparison/append touches another group's tables opts into being
    registered in THAT group's pipeline instead (see V-CYC-4 below) -- ``spec_group_id`` is
    this spec's own top-level ``dataflow_group_id``, needed only so that WARNING can name both
    groups involved, not just the one this flow declares.

    Deviation from the base design note: true cross-*spec* awareness (e.g. detecting that
    ``flowx_testing/003``'s ``append_target_table`` is ``flowx_testing/002``'s zerobus
    ingestion source, when 002 and 003 are onboarded as two separate spec documents) is out of
    reach for a pure ``validate_spec`` call, which only ever sees the flows of the ONE spec
    being validated -- there is no live Unity Catalog session or cross-group registry threaded
    into this function. What is implemented here is the in-spec facsimile: a reconciliation
    flow that declares a ``dataflow_group_id`` different from this spec's own is treated as
    "placed in a different group" for V-CYC-3/V-CYC-4 purposes. Genuine cross-spec detection
    belongs to a live preflight check against the control tables (see
    ``onboarding/uc_spec_preflight.py``), not to this offline, single-document validator.
    """
    if not isinstance(reconciliation_flows, list) or not reconciliation_flows:
        return

    # (qualified_casefolded, flow_label, cdc_load_strategy, target_type)
    in_spec_targets: List[Tuple[str, str, Optional[str], Optional[str]]] = []
    for flow in list(ingestion_flows or []) + list(transformation_flows or []):
        if not isinstance(flow, dict):
            continue
        qualified = _qualified_or_none(flow.get("target_catalog"), flow.get("target_schema"), flow.get("target_table"))
        if qualified is None:
            continue
        target_config = flow.get("target_config") if isinstance(flow.get("target_config"), dict) else {}
        flow_label = str(flow.get("dataflow_id") or flow.get("flow_step_id") or "<unknown flow>")
        in_spec_targets.append((qualified, flow_label, target_config.get("cdc_load_strategy"), flow.get("target_type")))

    # (qualified_casefolded, flow_label) -- zerobus is the only ingestion source_type addressed
    # as a qualified catalog.schema.table; autoloader/asn1 read from a filesystem path, which is
    # never a Lakeflow dataset reference.
    in_spec_ingestion_sources: List[Tuple[str, str]] = []
    for flow in list(ingestion_flows or []):
        if not isinstance(flow, dict) or flow.get("source_type") != "zerobus":
            continue
        source_config = flow.get("source_config") if isinstance(flow.get("source_config"), dict) else {}
        qualified = _qualified_or_none(
            source_config.get("source_catalog"), source_config.get("source_schema"), source_config.get("source_table")
        )
        if qualified is not None:
            in_spec_ingestion_sources.append((qualified, str(flow.get("dataflow_id") or "<unknown ingestion flow>")))

    spec_group = spec_group_id if isinstance(spec_group_id, str) and spec_group_id else None

    for flow in reconciliation_flows:
        if not isinstance(flow, dict):
            continue
        recon_id = flow.get("reconciliation_id", "<missing reconciliation_id>")
        label = f"reconciliation_flow[{recon_id}]"

        execution_mode = flow.get("execution_mode")
        if execution_mode not in ALLOWED_RECONCILIATION_EXECUTION_MODES:
            execution_mode = "job"
        pipeline_mode = execution_mode in _RECONCILIATION_PIPELINE_MODES

        declared_group = flow.get("dataflow_group_id")
        declared_group = declared_group if isinstance(declared_group, str) and declared_group else None
        effective_group = declared_group or spec_group
        same_group_as_spec = declared_group is None or declared_group == spec_group

        # V-CYC-6
        if pipeline_mode and declared_group is None:
            errors.append(
                f"{label}.dataflow_group_id: is required when execution_mode is {execution_mode!r} -- a "
                "group-less reconciliation flow has no Lakeflow pipeline to be registered into. Add a "
                "dataflow_group_id (this spec's own, to run inside its own pipeline update, or another "
                "group's, to run inside that group's pipeline instead), or keep execution_mode 'job'."
            )

        source_config = flow.get("source_config") if isinstance(flow.get("source_config"), dict) else {}
        source_ref = _qualified_table_ref_or_none(source_config.get("table"))
        producer = None
        if source_ref is not None and same_group_as_spec:
            for qualified, flow_label, cdc_load_strategy, target_type in in_spec_targets:
                if qualified == source_ref:
                    producer = (flow_label, cdc_load_strategy, target_type)
                    break

        # V-CYC-1
        if pipeline_mode and same_group_as_spec and source_ref is not None and producer is None:
            errors.append(
                f"{label}.source_config.table: does not resolve to any target this dataflow group actually "
                "produces (an ingestion_flows[]/transformation_flows[] target_catalog.target_schema."
                f"target_table) -- execution_mode {execution_mode!r} requires the reconciliation source to be "
                "a dataset THIS pipeline update publishes, so it reads this update's freshly written rows "
                "instead of silently degrading to an external, always-one-update-stale read. List the "
                "group's actual producing target, or set execution_mode to 'job'."
            )

        # V-CYC-7
        if pipeline_mode and producer is not None:
            _, cdc_load_strategy, target_type = producer
            if cdc_load_strategy in _RECONCILIATION_SOURCE_MERGE_CDC_STRATEGIES:
                errors.append(
                    f"{label}.source_config.table: is produced by cdc_load_strategy {cdc_load_strategy!r}, which "
                    f"dispatches through dlt.apply_changes[_from_snapshot] (real MERGE/UPDATE/DELETE writes, "
                    f"not append-only) -- execution_mode {execution_mode!r} requires an append-only producer. "
                    "Set execution_mode to 'job' to keep the standalone engine for this source."
                )
            elif cdc_load_strategy == "TRUNCATE_AND_LOAD" and target_type == "materialized_view":
                errors.append(
                    f"{label}.source_config.table: is produced by cdc_load_strategy 'TRUNCATE_AND_LOAD' into "
                    "target_type 'materialized_view', which is fully refreshed (not append-only) on every "
                    f"update -- execution_mode {execution_mode!r} requires an append-only producer. Set "
                    "execution_mode to 'job' to keep the standalone engine for this source."
                )

        target_configs = flow.get("target_configs") if isinstance(flow.get("target_configs"), list) else []
        # Same length as target_configs (None for a malformed non-dict entry), so other_index
        # below always addresses the true target_configs[] position, not a post-filter one.
        other_table_refs = [
            _qualified_table_ref_or_none(tc.get("table")) if isinstance(tc, dict) else None for tc in target_configs
        ]

        for index, target_config in enumerate(target_configs):
            if not isinstance(target_config, dict):
                continue
            target_path = f"{label}.target_configs[{index}]"
            append_ref = _qualified_table_ref_or_none(target_config.get("append_target_table"))
            if append_ref is None:
                continue

            # V-CYC-2
            target_match = next((flow_label for qualified, flow_label, _, _ in in_spec_targets if qualified == append_ref), None)
            if target_match is not None:
                message = (
                    f"{target_path}.append_target_table: is this dataflow group's own target '{target_match}' -- "
                    "Lakeflow owns that table's transaction log, so a reconciliation append into it here would "
                    "corrupt whatever write contract (streaming append, CDC MERGE, or MV refresh) that flow "
                    "already declared. Pick a different sink, or restructure the reconciliation as a "
                    "comparison against that target instead of an append into it."
                )
                _append_cycle_finding(pipeline_mode, execution_mode, message, errors)

            # V-CYC-3 / V-CYC-4
            source_match = next((flow_label for qualified, flow_label in in_spec_ingestion_sources if qualified == append_ref), None)
            if source_match is not None:
                if same_group_as_spec:
                    _append_cycle_finding(
                        pipeline_mode,
                        execution_mode,
                        f"{target_path}.append_target_table: is the raw ingestion source ingestion_flow"
                        f"[{source_match}] reads from, in this same dataflow group -- appending corrections "
                        "back into it races the next update's own read of it, corrupting an append-only "
                        "source's contract rather than healing a target.",
                        errors,
                    )
                else:
                    logger.warning(
                        "%s.append_target_table: is the raw ingestion source of ingestion_flow[%s] in "
                        "dataflow group %r, appended to by a reconciliation flow placed in dataflow group "
                        "%r -- Lakeflow cannot see this cross-pipeline landing -> ... -> append loop from "
                        "either pipeline's own graph, so it will never warn on its own. Confirm this is an "
                        "intended correction feedback loop between the two groups, not an accidental one.",
                        target_path,
                        source_match,
                        spec_group,
                        effective_group,
                    )

            # V-CYC-5
            if source_ref is not None and append_ref == source_ref:
                _append_cycle_finding(
                    pipeline_mode,
                    execution_mode,
                    f"{target_path}.append_target_table: must not equal this flow's own source_config.table -- "
                    "appending corrections back into the dataset being compared re-arms every future run "
                    "against its own output.",
                    errors,
                )
            for other_index, other_ref in enumerate(other_table_refs):
                if other_ref is None or other_ref != append_ref:
                    continue
                if other_index == index:
                    _append_cycle_finding(
                        pipeline_mode,
                        execution_mode,
                        f"{target_path}.append_target_table: must not equal this same target's own "
                        f"target_configs[{index}].table -- appending corrections into the dataset being "
                        "compared re-arms every future run against its own output.",
                        errors,
                    )
                else:
                    _append_cycle_finding(
                        pipeline_mode,
                        execution_mode,
                        f"{target_path}.append_target_table: must not equal target_configs[{other_index}].table -- "
                        "appending corrections into a dataset this same flow also reconciles against re-arms "
                        "every future run against its own output.",
                        errors,
                    )


def validate_spec(
    spark: SparkSession, spec: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
    """Validate the full onboarding spec: structure, types, allowed values, and SQL syntax.

    Collects *all* validation problems rather than failing fast, so a single onboarding
    attempt surfaces a complete error report instead of one issue at a time. This function
    never raises on validation findings -- only on truly unexpected internal errors -- the
    caller is responsible for raising on a non-empty ``errors`` list (see
    ``02_onboarding_engine.py``).

    Note: ``depends_on_dataflow_group_ids`` is no longer a valid field -- orchestration
    (dependency ordering between dataflow groups) is Lakeflow Jobs' responsibility, not the
    framework's. Any spec still carrying it is silently ignored (not an error) so pre-migration
    specs don't hard-fail on a field that's simply obsolete now -- see docs/01's Migration Notes.

    ``observability`` (telemetry destinations for the DLT observability engine, see
    ``docs/25_dlt_observability_module.md``) is validated here too, alongside every other
    top-level flow array, and upserted from the same spec by ``metadata_upsert.py::
    upsert_observability_config`` -- there is no separate observability config file.

    ``spark_config`` (group-scoped ``spark.conf`` settings, persisted to
    ``dataflow_group_spec.spark_config_json``) is validated here but is deliberately NOT returned:
    it is group-level metadata, not a flow array, so it travels to the control table through the
    caller's own ``spec`` dict rather than through this function's return tuple -- exactly like
    ``pipeline_parameters`` already does.

    Returns
    -------
    tuple
        ``(ingestion_flows, transformation_flows, reconciliation_flows, observability_destinations, errors)``.
    """
    errors: List[str] = []

    reject_unknown_keys(spec, "", errors, ALLOWED_ROOT_KEYS)
    check_string(spec.get("dataflow_group_id"), "dataflow_group_id", errors, required=True)
    pipeline_parameters = spec.get("pipeline_parameters") or {}
    if spec.get("pipeline_parameters") is not None:
        check_dict(spec.get("pipeline_parameters"), "pipeline_parameters", errors)
    if spec.get("spark_config") is not None:
        _validate_spark_config(spec.get("spark_config"), "spark_config", errors)
    if spec.get("source_plane") is not None:
        _validate_source_plane(spec.get("source_plane"), "source_plane", errors)

    ingestion_flows = spec.get("ingestion_flows", []) or []
    transformation_flows = spec.get("transformation_flows", []) or []

    if not isinstance(ingestion_flows, list):
        errors.append(f"ingestion_flows: expected a list, got {ingestion_flows!r}")
        ingestion_flows = []
    if not isinstance(transformation_flows, list):
        errors.append(f"transformation_flows: expected a list, got {transformation_flows!r}")
        transformation_flows = []

    reconciliation_flows = _validate_reconciliation_flows(spark, spec.get("reconciliation_flows"), pipeline_parameters, errors)
    observability_destinations = _validate_observability_destinations(spec.get("observability"), errors)

    if not ingestion_flows and not transformation_flows and not reconciliation_flows:
        errors.append(
            "At least one of 'ingestion_flows', 'transformation_flows', or 'reconciliation_flows' must be non-empty"
        )

    for flow in ingestion_flows:
        flow_id = flow.get("dataflow_id", "<missing dataflow_id>") if isinstance(flow, dict) else "<not an object>"
        label = f"ingestion_flow[{flow_id}]"
        if not check_dict(flow, label, errors, required=True):
            continue

        reject_unknown_keys(flow, label, errors, ALLOWED_INGESTION_FLOW_KEYS)
        check_string(flow.get("dataflow_id"), f"{label}.dataflow_id", errors, required=True)
        check_string(flow.get("source_type"), f"{label}.source_type", errors, required=True, allowed_values=ALLOWED_SOURCE_TYPES)
        check_string(flow.get("target_catalog"), f"{label}.target_catalog", errors, required=True)
        check_string(flow.get("target_schema"), f"{label}.target_schema", errors, required=True)
        check_string(flow.get("target_table"), f"{label}.target_table", errors, required=True)
        check_string(flow.get("target_type"), f"{label}.target_type", errors, required=True, allowed_values=ALLOWED_TARGET_TYPES)

        _validate_ingestion_source_config(
            flow.get("source_type"),
            flow.get("source_config"),
            f"{label}.source_config",
            errors,
            target_catalog=flow.get("target_catalog"),
            target_table=flow.get("target_table"),
        )
        _validate_path_parameters(flow.get("source_config"), pipeline_parameters, f"{label}.source_config", errors)
        cdc_load_strategy = _validate_target_config(flow.get("target_config"), f"{label}.target_config", errors, flow.get("target_type"))
        _validate_path_parameters(flow.get("target_config"), pipeline_parameters, f"{label}.target_config", errors)
        if cdc_load_strategy == "SCD3":
            errors.append(
                f"{label}.target_config.cdc_load_strategy: 'SCD3' is only valid for transformation_flows, not "
                "ingestion_flows (SCD3 pivots current/previous state via an internal history table, which only "
                "makes sense downstream of a raw ingestion flow)"
            )
        _validate_dq_config(flow.get("dq_config"), f"{label}.dq_config", errors)
        _validate_governance_tags(flow.get("governance_tags"), f"{label}.governance_tags", errors)

    for flow in transformation_flows:
        flow_id = flow.get("flow_step_id", "<missing flow_step_id>") if isinstance(flow, dict) else "<not an object>"
        label = f"transformation_flow[{flow_id}]"
        if not check_dict(flow, label, errors, required=True):
            continue

        reject_unknown_keys(flow, label, errors, ALLOWED_TRANSFORMATION_FLOW_KEYS)
        check_string(flow.get("flow_step_id"), f"{label}.flow_step_id", errors, required=True)
        check_string(flow.get("dataflow_id"), f"{label}.dataflow_id", errors, required=True)
        check_string(flow.get("target_catalog"), f"{label}.target_catalog", errors, required=True)
        check_string(flow.get("target_schema"), f"{label}.target_schema", errors, required=True)
        check_string(flow.get("target_table"), f"{label}.target_table", errors, required=True)
        check_string(flow.get("target_type"), f"{label}.target_type", errors, required=True, allowed_values=ALLOWED_TARGET_TYPES)
        check_string(flow.get("transformation_sql"), f"{label}.transformation_sql", errors, required=True)

        _validate_source_inputs(flow.get("source_inputs"), f"{label}.source_inputs", errors)
        _validate_target_config(flow.get("target_config"), f"{label}.target_config", errors, flow.get("target_type"))
        _validate_path_parameters(flow.get("target_config"), pipeline_parameters, f"{label}.target_config", errors)
        _validate_dq_config(flow.get("dq_config"), f"{label}.dq_config", errors)
        _validate_governance_tags(flow.get("governance_tags"), f"{label}.governance_tags", errors)

        if isinstance(flow.get("transformation_sql"), str) and flow.get("transformation_sql"):
            _validate_sql_syntax(spark, flow["transformation_sql"], label, pipeline_parameters, errors)

    _validate_no_duplicate_input_names(transformation_flows, errors)
    _validate_reconciliation_pipeline_placement(
        spec.get("dataflow_group_id"), ingestion_flows, transformation_flows, reconciliation_flows, errors
    )
    # V-CYC-8 is a pure ingestion rule -- two flows landing on one raw path while disagreeing
    # about landing_retention_policy/source_zip_handling. It is called here rather than from
    # inside _validate_reconciliation_pipeline_placement (where it shipped in v1.5.0) because
    # that function early-returns when the spec declares no reconciliation flows, which made
    # the rule dead for the ordinary Auto Loader spec it exists to protect.
    _validate_landing_side_effect_collisions(ingestion_flows, errors)

    return ingestion_flows, transformation_flows, reconciliation_flows, observability_destinations, errors
