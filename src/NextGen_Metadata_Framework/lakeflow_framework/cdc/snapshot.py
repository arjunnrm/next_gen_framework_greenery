"""Full-snapshot CDC via ``dlt.apply_changes_from_snapshot``, keyed on ``primary_keys``.

Dispatched from ``cdc/dispatcher.py::register_cdc_strategy`` for ``FULL_SNAPSHOT_CDC``. Each
newly-landed full extract is diffed against the previous one to derive inserts/updates/deletes,
using ``target_config.primary_keys`` as the diff key. An optional ``cdc_operation_column``/
``cdc_operation_mapping.delete_values`` lets a snapshot source mark its own rows as deletes;
those rows are filtered out of the snapshot before ``apply_changes_from_snapshot`` ever compares
it against the prior version.

Databricks-native only, as of v1.4.0
------------------------------------
``FULL_SNAPSHOT_CDC_NO_PK`` is **removed**, and with it this module's entire non-native path.
That strategy had no key, so the framework manufactured one: ``__framework_surrogate_key``, a
SHA-256 over every payload column of every row, recomputed on every run and forced on even
against an explicit ``generate_surrogate_key: false``. It cost a full-width hash per row per
snapshot, it made row identity depend on the exclusion list staying correct (a single volatile
column leaking into the hash re-keyed the entire table and made ``apply_changes_from_snapshot``
read it as a delete-and-reinsert of everything -- a defect this framework actually shipped and
had to fix), and it existed only to satisfy a ``keys=`` argument the source could not supply.

What remains is exactly what Databricks documents at
https://docs.databricks.com/aws/en/ldp/cdc: ``apply_changes_from_snapshot`` over a real key.
Sources that genuinely have no key use ``TRUNCATE_AND_LOAD``, which replaces the target
wholesale and needs no key to do it -- an honest full refresh instead of a synthetic diff.
``onboarding/spec_validator.py`` rejects the withdrawn strategy by name and says so.
"""

import logging
from typing import Any, Dict

import dlt
from pyspark.sql import functions as F

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import CdcStrategyError
from NextGen_Metadata_Framework.lakeflow_framework.storage.table_properties import qualified_table_name

logger = logging.getLogger("common.cdc.snapshot")


def register_full_snapshot_cdc(
    flow_id: str,
    source_view: str,
    target_table: str,
    target_catalog: str,
    target_schema: str,
    target_config: Dict[str, Any],
    table_properties: Dict[str, str],
    cdc_load_strategy: str,
    is_streaming: bool = False,
) -> None:
    """Register ``apply_changes_from_snapshot`` for full-extract CDC on a natural key.

    ``target_config.primary_keys`` is the diff key and the only diff key -- there is no
    framework-generated substitute since v1.4.0 (see this module's docstring). Published under
    ``target_catalog.target_schema`` (see
    ``common.storage.table_properties.qualified_table_name``).

    ``cdc_load_strategy`` is still taken as a parameter, and still threaded through into error
    messages, even though ``FULL_SNAPSHOT_CDC`` is now the only value that reaches here: the
    dispatch table in ``cdc/dispatcher.py`` calls every strategy module with one uniform
    signature, and an error that names the strategy an operator actually wrote is worth more than
    one that assumes.

    Raises
    ------
    CdcStrategyError
        If ``target_config.primary_keys`` is missing/empty, or if registration otherwise fails.
    """
    keys = target_config.get("primary_keys", [])
    if not keys:
        raise CdcStrategyError(
            f"Flow '{flow_id}': {cdc_load_strategy} requires target_config.primary_keys -- "
            "apply_changes_from_snapshot diffs successive snapshots on a key, and as of v1.4.0 the "
            "framework no longer manufactures one. Declare the source's natural key, or switch this "
            "flow to TRUNCATE_AND_LOAD if it genuinely has none."
        )

    cdc_op_column = target_config.get("cdc_operation_column")
    cdc_op_mapping = target_config.get("cdc_operation_mapping")  # e.g. {"delete_values": ["D", "X"]}

    # The snapshot input is registered as its own dataset rather than built inside a lambda
    # handed to apply_changes_from_snapshot. That is not a style choice -- Lakeflow forbids the
    # lambda form here, in two different ways, and both were observed live on 2026-08-29:
    #
    #   1. A lambda that calls dlt.read(source_view) on a pipeline-local VIEW fails analysis with
    #      TABLE_OR_VIEW_NOT_FOUND naming the fully-qualified dataset -- the lambda runs outside
    #      graph-element registration, so the bare name is qualified against the flow's target
    #      catalog/schema and resolved through the metastore, which no view can satisfy.
    #   2. Materializing that dataset so the metastore lookup CAN succeed then fails with
    #      REFERENCE_DLT_DATASET_OUTSIDE_QUERY_DEFINITION: "Referencing pipeline dataset ...
    #      outside the dataset query definition (i.e., @dlt.table annotation) is not supported.
    #      Please read it instead inside the dataset query definition."
    #
    # Taken together those say a snapshot lambda may not reference ANY dataset belonging to this
    # pipeline, materialized or not. So the read, the delete-value filter and the key-presence
    # guard all move inside a real dataset query definition -- where dlt.read() is legal -- and
    # apply_changes_from_snapshot is given that dataset's NAME instead of a callable.
    #
    # It is registered as a @dlt.table (materialized), not a @dlt.view, because
    # apply_changes_from_snapshot diffs successive snapshots and stamps each with a version.
    # That presupposes a stable, versioned Delta relation; a view is recomputed per access and
    # has no version to anchor the diff to.
    # KNOWN LIMITATION -- successive snapshots accumulate.
    #
    # apply_changes_from_snapshot treats this dataset's *current contents* as "the latest full
    # snapshot" and deletes any key absent from it. That is exactly right for a single snapshot,
    # and wrong once a second one arrives through a STREAMING upstream: an Auto Loader source
    # appends each new file's rows to this dataset rather than replacing them, so after a Day-2
    # extract lands, this dataset holds Day-1 UNION Day-2. Keys dropped in Day-2 are still present,
    # so they are never deleted from the target, and keys modified in Day-2 appear twice.
    #
    # Single-snapshot flows (and batch upstreams whose landing directory is overwritten in place)
    # behave correctly. Multi-snapshot diffing over an append-only landing zone -- TC-CDC-006's and
    # TC-CDC-007's Day-1/Day-2 scenario -- does NOT, and cannot be made to with a named dataset,
    # because no named dataset can mean "only the most recently arrived snapshot".
    #
    # The supported Lakeflow pattern for that case is the lambda form of
    # apply_changes_from_snapshot reading a versioned PATH per snapshot (spark.read.load(
    # f"{base}/v{n}") -- legal inside the lambda precisely because a path is not a pipeline
    # dataset, unlike the dlt.read() calls that fail with
    # REFERENCE_DLT_DATASET_OUTSIDE_QUERY_DEFINITION). Adopting it requires a new spec contract for
    # locating each snapshot version, and changes how DQ/quarantine applies to snapshot flows, so
    # it is deliberately not attempted here.
    snapshot_input = f"_{target_table}_snapshot_input"

    @dlt.table(
        name=snapshot_input,
        comment=f"Snapshot input to apply_changes_from_snapshot for {target_table}",
    )
    def _snapshot_input():
        # read_stream vs read must match how the clean upstream was registered. Lakeflow enforces
        # this: reading a streaming view with the batch dlt.read() fails analysis with "View
        # `<name>` is a streaming view and must be referenced using readStream" (TC-CDC-006 and
        # TC-CDC-007, 2026-08-29). is_streaming is threaded down from flow_registration.py rather
        # than guessed here, because it is the same flag dq/quarantine.py used to decide which of
        # the two it registered.
        snapshot_df = dlt.read_stream(source_view) if is_streaming else dlt.read(source_view)
        # Defense in depth, evaluated here because this closure is the first moment source_view
        # has a schema. Without it, a key column that never reached the clean upstream (renamed by
        # column_normalization, dropped by a data_standardization_sql projection) fails inside
        # apply_changes_from_snapshot with a generic missing-key error that names neither the
        # column nor the spec field it came from.
        #
        # This guard used to check for __framework_surrogate_key on a NO_PK flow. It now checks
        # the configured primary_keys instead -- the same defect class, one layer earlier: with no
        # generated key to fall back on, a mistyped primary_keys entry IS the failure.
        missing_keys = [key for key in keys if key not in snapshot_df.columns]
        if missing_keys:
            raise CdcStrategyError(
                f"Flow '{flow_id}': {cdc_load_strategy} requires target_config.primary_keys "
                f"{keys} on its source view, but {missing_keys} are not present. Available columns: "
                f"{sorted(snapshot_df.columns)}. Check for a rename applied by "
                "source_config.column_normalization or a data_standardization_sql projection that "
                "drops the key."
            )
        # Dropping delete-flagged rows from the snapshot is equivalent to deleting them from the
        # target: apply_changes_from_snapshot removes any key absent from the incoming snapshot.
        if cdc_op_column and cdc_op_mapping and cdc_op_mapping.get("delete_values"):
            snapshot_df = snapshot_df.filter(~F.col(cdc_op_column).isin(cdc_op_mapping["delete_values"]))
        return snapshot_df

    try:
        qualified_target = qualified_table_name(target_catalog, target_schema, target_table)
        dlt.create_streaming_table(name=qualified_target, table_properties=table_properties)
        dlt.apply_changes_from_snapshot(
            target=qualified_target,
            source=snapshot_input,
            keys=keys,
            stored_as_scd_type="1",
        )
    except Exception as exc:  # noqa: BLE001
        raise CdcStrategyError(
            f"Flow '{flow_id}': failed to register {cdc_load_strategy} target '{target_table}': {exc}"
        ) from exc
