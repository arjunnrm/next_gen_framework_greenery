---
name: flowx-onboarding
description: Answer questions about, guide, and generate FlowX onboarding specs (the JSON/YAML that drives flowx pipelines) for this repo. Use whenever the task involves an onboarding spec, dataflow_group_id, ingestion_flows, transformation_flows, reconciliation_flows, source_type/target_type/cdc_load_strategy, dq_config, governance_tags, or the FlowX control tables.
---

# FlowX onboarding specs

This skill covers the onboarding spec: the JSON (or YAML) document that declares a
`dataflow_group_id` and its flows, is validated by `onboarding/spec_validator.py`, upserted into
the control tables, and then drives a Lakeflow pipeline.

## The one rule that matters most

**Never hand the user a generated spec you have not validated.** The framework rejects unknown
attributes as of v1.7.1, so a spec that "looks right" is not evidence of anything. Validation is
offline, needs no cluster, and takes under a second:

```bash
python -c "
import json,sys; sys.path.insert(0,'src')
from flowx.lakeflow_framework.onboarding.agent_tools import validate_json
r = validate_json(open('my_spec.json', encoding='utf-8').read())
print(r['summary'])
[print(' ERROR:', e) for e in r['errors']]
[print(' warn :', w) for w in r['warnings']]
"
```

Loop until `valid` is `True`, then show the user the spec **and** the passing result. Error
messages name the correct attribute (`Use dq_config.`, `Use target_config.partition_columns.`),
so fix from the message rather than guessing again.

`validate_json` accepts JSON or YAML, substitutes `{{catalog}}`/`{{env}}`, runs the full
framework validator, and adds governance warnings. With `spark=None` it skips only the
`EXPLAIN`-based SQL check, which runs later on the cluster.

## Do not invent attributes

Every attribute is enumerated. The framework reads no others, and since v1.7.1 an unrecognised
key is a hard error rather than a silent no-op — because an ignored `data_quality` block meant
data quality never ran, with nothing anywhere to say so.

Names that get invented, and what to use instead:

| Wrong | Right |
|---|---|
| `data_quality`, `quality_config`, `expectations` | `dq_config` (`{rules[], quarantine_table, record_id_column}`) |
| `cdc_config: {...}` | `target_config.cdc_load_strategy` + `primary_keys` / `sequence_by_column` |
| `file_format` | `source_config.format` |
| `infer_schema`, `schema_inference` | nothing — delete it; use `schema_location` / `schema_config_path` |
| `partition_by` | `target_config.partition_columns` |
| `cluster_by` | `target_config.liquid_clustering_columns` |
| `primary_key` (scalar) | `target_config.primary_keys` (list, even for one column) |
| `merge_keys` | `target_config.primary_keys` |
| `sequence_by` | `target_config.sequence_by_column` |
| `tags` | `governance_tags.table_tags` |
| `table_comment` | `target_config.table_properties` |
| `source_path` | `source_config.path` |
| `target_path` | `target_config.sink_config.path` (sink targets only) |
| `normalize_columns`, `normalize_column_names` | `source_config.column_normalization` (`{enabled, case}`) |
| `sql`, `query` | `transformation_sql` (transformation) / `transform_sql` (reconciliation) |
| `flow_id` | `dataflow_id` (ingestion) / `flow_step_id` (transformation) |
| `depends_on_dataflow_group_ids` | nothing — ordering is a Lakeflow Jobs `depends_on`, not a spec key |

Keys starting with `_` are always allowed as author comments (`_scenario`, `_provenance`), as is
`$schema`. Use them freely to explain a spec; JSON has no comment syntax.

## Start from a golden example, not from memory

`references/golden_specs.json` holds five complete, machine-validated specs — every one is
asserted valid by `tests/unit/test_golden_specs.py`, so they cannot rot:

| Example | Use when |
|---|---|
| `01_minimal_autoloader_append` | Land files into a Bronze streaming table, append-only |
| `02_scd2_with_dq_and_tags` | SCD2 history plus data-quality rules and governance tags |
| `03_transformation_join` | Join upstream tables into a Gold materialized view |
| `04_zerobus_scd1` | Stream an existing Delta table, latest row per key |
| `05_reconciliation_in_pipeline` | Compare source and target inside the pipeline DAG |

Copy the closest one and change the values. That is far more reliable than composing a spec
field by field.

## Spec shape

```
dataflow_group_id        required, string, conventionally dfg_*
pipeline_parameters      optional, {name: value}, referenced as ${name}
spark_config             optional, group-scoped spark.conf settings
source_plane             optional
ingestion_flows[]        \
transformation_flows[]    >  at least one must be non-empty
reconciliation_flows[]   /
observability[]          optional; does NOT satisfy the "at least one" rule
```

Ingestion and transformation flows share a shape. Ingestion has `source_type` + `source_config`;
transformation has `source_inputs[]` + `transformation_sql` and reads only from tables. Both need
`target_catalog`, `target_schema`, `target_table`, `target_type`, and a `target_config` carrying
`cdc_load_strategy`.

Allowed values (from the validator's own constants — the authority):

- `source_type`: `autoloader`, `zerobus`, `asn1`
- `target_type`: `streaming_table`, `materialized_view`, `batch_table`, `external_sink`, `sink`
- `cdc_load_strategy`: `APPEND`, `SCD1`, `SCD2`, `SCD3`, `TRUNCATE_AND_LOAD`, `FULL_SNAPSHOT_CDC`
  (`SCD3` is transformation-only; `FULL_SNAPSHOT_CDC_NO_PK` was removed)
- `dq_config.rules[].action`: `warn`, `drop`, `fail`, `quarantine`

For anything beyond this — every field with types, defaults and worked examples — read
[`references/spec_reference.json`](references/spec_reference.json) and the deep-dive map in
[`references/framework_guide.md`](references/framework_guide.md).

## Answering questions about the framework

Read the file, do not recall it. [`references/framework_guide.md`](references/framework_guide.md)
is the map (architecture, sources, targets, CDC, sinks, reconciliation, secrets, governance, DQ,
observability) and links to the authoritative source for each topic.
[`references/common_pitfalls.md`](references/common_pitfalls.md) collects failures this repo has
actually hit — check it before asserting that something works.

When a question is about what is *enforced*, the answer is in
`src/flowx/lakeflow_framework/onboarding/spec_validator.py`: the
`ALLOWED_*` / `REMOVED_*` constants near the top are the ground truth, and they are asserted
against the JSON schema by `tests/unit/test_unknown_key_rejection.py`.

## Onboarding a validated spec

Deploy the bundle, then run the generic onboarding job with the spec path — never inline
`02_onboarding_engine.py` into a new job. See
[`references/framework_guide.md`](references/framework_guide.md) for the full walkthrough, and
note that a `bundle deploy` issued while a pipeline is updating kills that update.
