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

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import SparkSession

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError
from NextGen_Metadata_Framework.lakeflow_framework.transformation.parameters import (
    substitute_dynamic_parameters,
    substitute_path_parameters,
)

logger = logging.getLogger("NextGen_Metadata_Framework.lakeflow_framework.onboarding.spec_validator")

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
ALLOWED_SINK_WRITE_MODES = {"overwrite", "append"}
ALLOWED_RECONCILIATION_FAILURE_MODES = {"fail", "warn"}
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
        "recon_mode widget; delete the key. For continuous coverage, schedule the reconciliation "
        "job on the cadence you need."
    ),
    "generate_surrogate_key": (
        "removed in v1.4.0 with the surrogate-key engine. A reconciliation flow matches on its "
        "declared match_keys; both sides must carry those columns."
    ),
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


ALLOWED_COMPARISON_DIRECTIONS = {"source_to_target", "target_to_source", "both"}
ALLOWED_READ_MODES = {"batch", "streaming"}
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
ALLOWED_PRE_EXTRACTION_DECRYPTION_TYPES = {"pgp"}

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
                if archive_config.get("secret") is not None:
                    # Optional AES password protection on the ZIP itself (archive/zip_utils.py),
                    # independent of -- and combinable with -- pgp_encryption below.
                    check_secret_ref(archive_config.get("secret"), f"{path_prefix}.post_export_archive.secret", errors)
                pgp_encryption = archive_config.get("pgp_encryption")
                if pgp_encryption is not None and check_dict(pgp_encryption, f"{path_prefix}.post_export_archive.pgp_encryption", errors):
                    pgp_path = f"{path_prefix}.post_export_archive.pgp_encryption"
                    check_bool(pgp_encryption.get("enabled"), f"{pgp_path}.enabled", errors, required=True)
                    if pgp_encryption.get("enabled"):
                        check_secret_ref(
                            pgp_encryption.get("recipient_public_key_secret"), f"{pgp_path}.recipient_public_key_secret", errors, required=True
                        )
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
        if decryption_type == "pgp":
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
        check_string(source_config.get("asn1_pdu_name"), f"{path_prefix}.asn1_pdu_name", errors, required=True)
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


def _validate_reconciliation_dataset_config(config: Any, path_prefix: str, errors: List[str]) -> None:
    """Validate one reconciliation side (``source_config`` or a ``target_configs[]`` entry).

    As of v1.3.0 the only supported ``type`` is ``"table"`` -- see
    ``ALLOWED_RECON_DATASET_TYPES`` for why ``"file"``/``"sink"`` were dropped. The explicit
    scope error below is emitted *in addition to* the generic allowed-values message
    ``check_string`` produces, because the generic one ("allowed values are ['table']") tells an
    author what is legal but not what to do about the spec they already have; the scope error
    names the migration (read the file/sink output into a Delta table first).
    """
    if not check_dict(config, path_prefix, errors, required=True):
        return

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


def _validate_reconciliation_target_configs(target_configs: Any, path_prefix: str, errors: List[str]) -> None:
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

        _validate_reconciliation_dataset_config(target_config, target_path, errors)

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
        _validate_reconciliation_dataset_config(flow.get("source_config"), f"{label}.source_config", errors)
        _validate_path_parameters(flow.get("source_config"), parameters, f"{label}.source_config", errors)
        _validate_reconciliation_target_configs(flow.get("target_configs"), f"{label}.target_configs", errors)
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

        _validate_logging_config(flow.get("logging_config"), f"{label}.logging_config", errors)

    return reconciliation_flows


def _validate_logging_config(logging_config: Any, label: str, errors: List[str]) -> None:
    """Validate the optional ``run_log_capture``/``mismatch_log_capture`` gates a reconciliation
    flow can set to skip ``reconciliation_run_log``/``reconciliation_mismatch_log`` writes for a
    high-frequency continuous flow. Both default to ``true`` (today's unconditional behavior) --
    absent is valid and changes nothing. ``reconciliation_result`` is always written regardless
    of this config and has no opt-out -- see reconciliation/appender.py.

    This block is the **onboarded per-flow layer**. It is overridden at run time by the
    ``recon_run_log_capture``/``recon_mismatch_log`` job parameters (see
    ``notebooks/05_reconciliation/05_reconciliation_engine.py`` and
    ``reconciliation/appender.py::resolve_log_capture_flags``), which are tri-state -- unset
    defers to exactly this config. The two naming schemes are deliberately distinct rather than
    unified: an operator firefighting a runaway flow needs to silence log writes for one run
    without re-onboarding, and needs to be able to tell at a glance whether a value came from
    metadata or from the run they just launched."""
    if logging_config is None:
        return
    if not check_dict(logging_config, label, errors):
        return
    if logging_config.get("run_log_capture") is not None:
        check_bool(logging_config.get("run_log_capture"), f"{label}.run_log_capture", errors)
    if logging_config.get("mismatch_log_capture") is not None:
        check_bool(logging_config.get("mismatch_log_capture"), f"{label}.mismatch_log_capture", errors)


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

    check_string(spec.get("dataflow_group_id"), "dataflow_group_id", errors, required=True)
    pipeline_parameters = spec.get("pipeline_parameters") or {}
    if spec.get("pipeline_parameters") is not None:
        check_dict(spec.get("pipeline_parameters"), "pipeline_parameters", errors)
    if spec.get("spark_config") is not None:
        _validate_spark_config(spec.get("spark_config"), "spark_config", errors)

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

    return ingestion_flows, transformation_flows, reconciliation_flows, observability_destinations, errors
