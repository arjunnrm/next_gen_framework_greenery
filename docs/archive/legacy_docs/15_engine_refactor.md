# Engine Refactor: Shared Flow Registration

> See also: [README.md](README.md) for the full FlowX documentation set.

## Purpose

Close the "avoid duplicated logic" gap between FlowX's ingestion and transformation
engines: both built an identical staged-view -> main/quarantine-table -> CDC-dispatch
shape independently, differing only in how they construct the staged DataFrame.

**Status note (read this first):** the six bugs below were found and fixed during the
refactor that centralized this shape into `engine/flow_registration.py`, described here as
project history. A later pass in the same session (Phase 7 -- "Sink rebuild", see the new
§ below) built directly on top of this refactor to add genuine `dlt.create_sink`/
`@dlt.append_flow` support for `target_type: "sink"`/`"external_sink"`. That pass **reversed
part of bug five's fix**: `register_flow_output` regained a `target_type` parameter (removed
by bug five, reinstated by Phase 7) because sink dispatch needs it. The bug-by-bug narrative
below is kept verbatim as accurate history of what was found and why -- the "Phase 7: Sink
Dispatch" section afterward documents the current end state, which supersedes anything below
that a later phase changed.

## Responsibilities

`engine/flow_registration.py` provides the two functions every flow-registration path
(ingestion, transformation) now calls instead of repeating the wiring inline:

* `register_staged_view(...)` — registers the `@dlt.view` staged intermediate: DQ
  expectations decorator, optional decrypt, optional encrypt, quarantine-column
  derivation (`add_quarantine_columns`). Takes a `build_dataframe` callable for the one
  thing that genuinely differs between engines (read a source vs. run SQL).
* `register_flow_output(...)` — dispatches on `target_type`. `"sink"` delegates entirely to
  `engine/sink_registration.py::register_sink_target` (no main/quarantine table at all --
  see the "Phase 7: Sink Dispatch" section below). Every other `target_type` --
  `"streaming_table"`/`"materialized_view"`/`"batch_table"`/`"external_sink"` -- registers
  the main/quarantine table pair (`register_main_and_quarantine_tables`) and dispatches the
  configured CDC strategy (`register_cdc_strategy`) exactly the same way; `"external_sink"`
  additionally calls `sink_registration.py::register_external_sink_export` afterward -- see
  the fifth bug below for why `external_sink` used to be special-cased *out* of this shared
  path entirely, and why that was wrong, and Phase 7 below for how its export half is
  wired up today.

`engine/run_context.py::resolve_pipeline_run_id` is a small companion: best-effort
resolution of a run/update identifier (tries a short list of known Spark conf keys,
falls back to `dataflow_group_id`), used to populate `__framework_pipeline_run_id` on quarantined
rows.

## Inputs / Outputs

Same as before the refactor — see
[03_engine_execution_flow.md](03_engine_execution_flow.md) for the full flow. This
change is purely internal: `notebooks/03_engine/03_lakeflow_declarative_pipeline.py`'s
`generate_ingestion_flow`/`generate_transformation_flow` shrank to "build the staged
DataFrame, then call the two shared functions" — no change to what gets registered or
in what order.

**Current `_build_ingestion_dataframe` shape** (added after this refactor, alongside other
work in this same pass): `generate_ingestion_flow`'s `build_dataframe` callable is
`read_ingestion_source` -> `attach_technical_metadata` -> `ingestion/json_flattening.py::
apply_explode_columns` -> `ingestion/standardization_sql.py::apply_data_standardization_sql`,
in that order -- explode/flatten runs before standardization so a `data_standardization_sql`
expression can reference a column that only exists after JSON explosion. This still ends at
"I have a staged DataFrame" before `register_staged_view` takes over (technical-metadata
timestamp, encryption, quarantine columns) -- the one per-engine seam this refactor
established is unchanged; explode/standardization are additional steps *inside* that same
seam, not a new one. `generate_transformation_flow`'s `build_dataframe` is unchanged: a bare
`lambda: spark.sql(resolved_sql)` (`data_standardization_sql`/`explode_columns` are
ingestion-only, see [01_control_metadata_schema.md](01_control_metadata_schema.md) §3).

## Design decisions

* **The DataFrame-construction callable is the only per-engine seam.** Everything after
  "I have a staged DataFrame" (DQ, encryption, quarantine, main/quarantine split, CDC
  dispatch) is identical between ingestion and transformation, and previously lived as
  two independent, drifting copies in the pipeline notebook. Centralizing it means a
  fix or new capability (e.g. this pass's quarantine metadata enrichment) is written
  once, not once-per-engine-and-hoping-they-stay-in-sync.
* **`pipeline_run_id` flows through `register_staged_view`, not `register_flow_output`.**
  It's baked into the staged view's columns by `add_quarantine_columns` before
  `register_flow_output` ever runs — threading it through a second time would be a dead
  parameter (this was caught and removed during the refactor itself; see the module's
  docstring for the reasoning).
* **A real bug found via live deployment: final tables were never qualified with their
  own `target_catalog`/`target_schema`.** A Lakeflow pipeline resource
  (`resources/*.yml`) sets exactly *one* default catalog/schema; a bare `name=`/`target=`
  passed to `@dlt.table`/`dlt.create_streaming_table`/`dlt.apply_changes` always resolves
  against *that* default, never against a flow's own `target_catalog`/`target_schema`
  control-table columns — confirmed against Lakeflow's multi-schema publishing support
  (a fully or partially qualified `name=` is required to publish outside the pipeline's
  default). Every final, externally-queryable dataset this framework registers (main
  table, quarantine table, SCD1/SCD2 streaming tables, the SCD2 `_current` reporting
  view, SCD3's target and hidden `_..._scd2_history` table, full-snapshot CDC tables) now
  goes through `storage/table_properties.py::qualified_table_name(catalog, schema,
  table)` before reaching a decorator's `name=`/`target=`. A same-pipeline read of one of
  these tables (the SCD2 reporting view, SCD3's pivot) must reference it by that same
  qualified string, not the bare `target_table` — `register_scd2_reporting_view` and
  `register_scd3` were updated accordingly. Internal, graph-only staged views
  (`_<target_table>_staged`) are deliberately left unqualified: they're never queried
  outside the pipeline's own graph, so there's nothing to gain from qualifying them.
  Every `register_*` function in `cdc/scd.py`, `cdc/snapshot.py`, `cdc/dispatcher.py`,
  and `dq/quarantine.py::register_main_and_quarantine_tables` gained `target_catalog`/
  `target_schema` parameters as part of this fix — a deliberate, documented breaking
  change per AGENTS.md §14 for any external caller of these engine internals (none exist
  outside this framework's own notebook).
* **A second real bug, found immediately after the first one via the same live
  deployment: `register_main_and_quarantine_tables` unconditionally published a
  `@dlt.table` under `target_table`'s own name, even for a flow whose
  `cdc_load_strategy` was SCD1/SCD2/SCD3/a snapshot-CDC strategy — and
  `cdc/dispatcher.py` then tried to publish a *second*, independent dataset under that
  identical name via `apply_changes`/`create_streaming_table`.** This raised Lakeflow's
  own `Cannot redefine dataset` error the moment a real SCD/CDC-strategy flow was
  actually run (the two prior sample pipelines had never exercised this path end to end).
  Compounding it, `register_cdc_strategy` read from the *raw* staged view rather than the
  quarantine-filtered one, so a configured quarantine rule had no effect at all on an
  SCD/CDC-strategy flow's target -- quarantined rows flowed straight into
  `apply_changes`. Both are fixed together: `register_main_and_quarantine_tables` now
  takes a `needs_cdc_dispatch: bool` and returns the dataset name the CDC dispatcher
  should read from. For `APPEND`/`TRUNCATE_AND_LOAD` (`needs_cdc_dispatch=False`) nothing
  changes -- the clean side is still the final `@dlt.table` published as `target_table`.
  For every other strategy, the clean side becomes an internal `@dlt.view`
  (`_<target_table>_clean`) instead, freeing `target_table`'s name for
  `cdc/dispatcher.py` to own, and `register_flow_output` passes that view's name through
  to `register_cdc_strategy` as its `source_view` -- so the SCD/CDC engine now correctly
  sees quarantine-filtered input.
* **A third real bug, found on the very next live deployment after fixing the second
  one: `@dlt.view` does not accept a multi-part qualified `name=` the way `@dlt.table`/
  `dlt.create_streaming_table` do.** Qualifying `register_scd2_reporting_view`'s own
  `_current` view the same way its underlying SCD2 *table* is qualified raised
  `AnalysisException: View with multipart name '...' is not supported` the moment an
  actual SCD2 flow ran. The fix is narrow: only ever qualify a *table*'s `name=`
  (`@dlt.table`, `create_streaming_table`, `apply_changes`/`apply_changes_from_snapshot`
  targets); every `@dlt.view` in this codebase (`register_staged_view`'s staged view, the
  quarantine module's `_<target_table>_clean` view, the SCD2 reporting view,
  `transformation/inputs.py`'s per-input views) stays on a bare, unqualified name --
  consistent with the fact that a view is never materialized or queryable outside the
  pipeline's own graph, so there is no "lands in the wrong schema" risk to guard against
  for one in the first place. The SCD2 reporting view still *reads* its underlying table
  by its qualified name (`dlt.read(qualified_target)`); only the view's own declared
  `name=` reverted to bare.
* **A fourth real bug, found once a transformation flow's SQL actually referenced a
  streaming `source_inputs` entry for the first time: `transformation_sql` executes as
  one `spark.sql(...)` call, which resolves a bare `FROM`/`JOIN <name>` as a *batch*
  reference no matter what kind of dataset `<name>` actually is.** A `source_inputs`
  entry with `is_streaming: true` is registered via `spark.readStream.table(...)`, and
  Lakeflow tracks it internally as a genuinely streaming dataset -- but nothing about the
  `transformation_sql` text itself signaled that, so the moment any SCD1/SCD2 flow with a
  streaming input was actually deployed (every one of Test Pipeline 1's five
  transformation flows, immediately after the third bug above was fixed), it raised
  `AnalysisException: View '...' is a streaming view and must be referenced using
  readStream`. The fix, `transformation/inputs.py::mark_streaming_references`, rewrites
  every `FROM`/`JOIN` reference to a streaming input into `FROM STREAM <name>`/`JOIN
  STREAM <name>` (the documented Spark SQL syntax for marking one specific table
  reference as streaming inside ad-hoc SQL text) before the SQL ever reaches
  `spark.sql(...)`. Deliberately an **engine** fix, not a spec-authoring convention: a
  first attempt at the very same error, for a single spec (`spec_06`, a windowed
  streaming aggregation), by hand-writing `FROM STREAM bronze_telemetry` in that one
  spec's `transformation_sql` worked -- but requiring every spec author to remember
  Spark's `STREAM` keyword whenever they reference a streaming input is exactly the kind
  of Python/Spark implementation detail AGENTS.md's config-only-onboarding goal is meant
  to eliminate, and it is easy to get wrong (this pass got it wrong across two whole spec
  files before generalizing the fix). The engine already knows which `source_inputs` are
  streaming; it now applies the rewrite automatically, and `spec_06`'s
  `transformation_sql` was reverted back to plain, unmarked SQL once the automatic path
  was confirmed to cover the exact case that originally motivated the manual one.
* **A fifth real bug, found the moment `run_pipeline_update` finally succeeded end to
  end for the first time this session and the downstream egress job task actually got
  to run: `register_flow_output` special-cased `target_type == "external_sink"` to
  register *only* the ephemeral staged `@dlt.view` and skip a physical table
  entirely.** `control_plane/post_deployment.py::run_external_sink_exports` then tried to
  `spark.read.table(...)` that staged view's name from a *separate, later* job task --
  which raised `TABLE_OR_VIEW_NOT_FOUND` no matter how the name was qualified, because a
  `@dlt.view` is never a durable, queryable catalog object once the pipeline update that
  defined it finishes (confirmed empirically: it never appears in `SHOW TABLES` for any
  schema afterward). The fix removes the special case entirely -- `external_sink` is now
  registered exactly like every other `target_type`, via the same
  `register_main_and_quarantine_tables`/`register_cdc_strategy` path, so it gets a real,
  qualified `@dlt.table` under `target_table`. `run_external_sink_exports` now reads
  `{target_catalog}.{target_schema}.{target_table}` instead of the old
  `_<target_table>_staged` guess. `register_flow_output`'s now-unused `target_type`
  parameter was removed along with the special case. **(Superseded -- see "Phase 7: Sink
  Dispatch" below.)** `run_external_sink_exports` and `control_plane/post_deployment.py`'s
  entire post-deployment egress path were later removed outright, and `register_flow_output`
  regained a `target_type` parameter, once `external_sink` egress moved from a
  post-deployment job task to a genuine in-graph Lakeflow sink -- this bug's own fix (a real,
  qualified `external_sink` main table) is still exactly how things work today; only *where*
  the export read from that table happens changed again after this refactor.
* **A sixth real bug, exposed by fixing the fifth: a transformation flow's staged view
  is read via `dlt.read_stream(...)` vs. `dlt.read(...)` in
  `register_main_and_quarantine_tables` based on `is_streaming`, which the notebook
  computed purely from `target_type == "streaming_table"` -- but a staged view built
  from `spark.sql(transformation_sql)` is a genuinely streaming computation whenever
  *any* of its `source_inputs` is streaming, entirely independent of the flow's own
  `target_type`.** An `external_sink` flow built from a windowed streaming aggregation
  (Test Pipeline 6's `heavy_usage_export`, `target_type: "external_sink"` fed by a
  streaming `bronze_telemetry` input) never surfaced this while `external_sink` was
  special-cased to skip `register_main_and_quarantine_tables` entirely (bug five) -- the
  moment that was fixed, it raised the exact same `AnalysisException: View '...' is a
  streaming view and must be referenced using readStream` as bugs three and four, one
  layer further down the same underlying "does the engine know this dataset is
  streaming" problem. Fixed in `generate_transformation_flow`:
  `is_streaming = target_type == "streaming_table" or any(i.get("is_streaming") for i in
  source_inputs)`. Ingestion flows are unaffected -- an ingestion source's
  streaming-ness already correlates 1:1 with `target_type == "streaming_table"` via
  `ingestion/readers.py`, with no equivalent per-input granularity to get wrong.

## Phase 7: Sink Dispatch (current end state)

A later pass in this same session built on top of this refactor to satisfy a stricter
requirement: "all external outputs must use genuine
[Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/) sink
functionality (`dlt.create_sink` + `@dlt.append_flow`), not ordinary DAG table writes;
sink nodes must not appear as persisted datasets." This closed the gap the fifth bug's fix left open --
`external_sink` got a real, qualified main table, but its *export* still ran as a separate,
later `control_plane/post_deployment.py::run_external_sink_exports` job task, which is
exactly the "separate post-deployment step racing the pipeline" anti-pattern the fifth bug
was already complaining about, just one layer further downstream (a plain
`spark.read.table(...).write.format(...).save(...)`, never actually part of the pipeline's
own DAG). `engine/sink_registration.py` is the fix, and `run_external_sink_exports` /
`control_plane/post_deployment.py`'s whole egress path is deleted, not deprecated.

**`register_flow_output`'s current dispatch** (`notebooks/03_engine/03_lakeflow_declarative_pipeline.py`
still passes `flow_row.target_type` as its 10th positional argument -- the parameter this
refactor's fifth-bug fix had removed as unused is back, because sink dispatch needs it):

* `target_type == "sink"`: delegates entirely to
  `sink_registration.py::register_sink_target`. **No main/clean table is ever registered** --
  the staged view feeds a genuine `dlt.create_sink`/`@dlt.append_flow` pair directly. Neither
  `register_main_and_quarantine_tables` nor CDC dispatch runs at all for this target type;
  its DQ quarantine table (if `dq_config.rules[]` has an `action: "quarantine"` entry) is
  still registered independently, reading the same staged view. Requires a genuinely
  streaming staged view -- `dlt.create_sink`/`@dlt.append_flow` is streaming-only (a real
  Lakeflow platform constraint, not this framework's choice) -- and raises
  `FrameworkConfigError` up front, naming the flow and the fix, rather than letting Lakeflow
  fail deep inside graph resolution.
* `target_type == "external_sink"`: unchanged from the fifth bug's fix through the main-table
  half (`register_main_and_quarantine_tables`/`register_cdc_strategy`, a real qualified
  table), **plus** a call to `sink_registration.py::register_external_sink_export`, which
  registers a *second*, separate `@dlt.append_flow` that reads the now-materialized main
  table (`dlt.read_stream(qualified_main_table)`) into its own `dlt.create_sink` -- defined
  at graph-definition time, in the same pipeline update, right alongside the main table it
  reads from. This requires the main table itself to be a genuine Streaming Table (true for
  every CDC-dispatched strategy except SCD3, whose public target is a derived batch pivot --
  see `cdc/scd.py::register_scd3` -- and true for `APPEND`/`TRUNCATE_AND_LOAD` exactly when
  the flow's own `is_streaming` is true); `register_external_sink_export` raises
  `FrameworkConfigError` naming the actual cause (e.g. `cdc_load_strategy: "SCD3"`) when it
  isn't.
* `"streaming_table"`/`"materialized_view"`/`"batch_table"`: unchanged -- exactly the shared
  path this refactor's Responsibilities section describes.

**`sink_config.format`** (`"delta"`/`"kafka"`/`"pgp_zip"`) determines what
`sink_registration.py::_build_sink_options` builds for `dlt.create_sink`; `"pgp_zip"` routes
through `archive/pgp_zip_sink.py`, a genuine custom
[PySpark Data Source](https://docs.databricks.com/aws/en/pyspark/datasources) Sink -- see
[23_lakeflow_sinks.md](23_lakeflow_sinks.md) for the full sink model, including why every
secret a `sink_config` needs is resolved eagerly at graph-definition time
(`_resolve_secret_into_options`) rather than lazily inside the sink's `write()`/`commit()`,
which run in a restricted worker process where `dbutils` fails (confirmed live -- see that
doc and `archive/pgp_zip_sink.py`'s module docstring).

## Error handling

No new exception types — `register_staged_view`/`register_flow_output` propagate
whatever the underlying calls raise (`FrameworkConfigError`, `CryptoError`,
`CdcStrategyError`) -- now also including `sink_registration.py`'s own `FrameworkConfigError`
raises for a non-streaming sink/export source or a malformed `sink_config` (Phase 7, see
above), unchanged in kind from before the refactor.

## Extension points

A third engine type (e.g. a future streaming-CDC-specific path) would implement its own
`build_dataframe` callable and call the same two shared functions — no new
staged-view/main-table/CDC-dispatch code required.

## Example usage

See `notebooks/03_engine/03_lakeflow_declarative_pipeline.py`'s
`generate_ingestion_flow`/`generate_transformation_flow` for the calling pattern.

## Relevant tests

Most of this module is pure DLT-graph-definition wiring, verifiable only via an actual
pipeline update -- exercised indirectly by every sample pipeline's integration tests.
`transformation/inputs.py::mark_streaming_references` is the one piece of genuinely
Spark-independent logic (plain string rewriting) and has its own unit coverage:
`tests/unit/test_transformation_inputs.py`. Phase 7's sink dispatch (the new section above)
has its own post-deployment integration coverage,
`tests/integration/test_lakeflow_sink_dag.py`, asserting against `spec_06`'s two sink flows:
that a `"sink"`-target table (`ts_iot_raw_events_direct_sink`) never exists as a queryable
object at all (the core "no persisted dataset" guarantee), and that an `"external_sink"`
flow (`ts_iot_heavy_usage_egress`) has both its real materialized table *and* at least one
exported PGP-ZIP archive file on disk.
