# Bronze Column Normalization & Explicit Schema Configuration

> See also: [README.md](README.md) — the full FlowX documentation set, and
> [01_control_metadata_schema.md](01_control_metadata_schema.md) for the full `source_config`
> field reference these two attributes belong to.

## Purpose

Two independent, opt-in `source_config` capabilities for ingestion flows (autoloader/zerobus/
asn1 — any `source_type`), both about column-name/schema hygiene at the point of Bronze
ingestion, before anything else in the flow ever sees the data:

1. **`normalize_column_names`** (boolean) — automatic trim/lowercase/replace-special-characters
   column-name cleanup. `ingestion/column_normalization.py`.
2. **`schema_config_path`** (string) — an external JSON/YAML file declaring explicit type
   mappings, nullability documentation, Unity Catalog column comments, and source-to-target
   field renames, for when automatic normalization isn't precise enough (or you need real
   type casts / comments, which normalization alone never does). `ingestion/schema_config.py`.

Both default to off — onboarding an existing flow with neither field set is a complete no-op,
identical to before either capability existed.

## Ordering: which runs first, and why it matters

```
read_ingestion_source()
        │
        ▼
apply_schema_config()        -- only if schema_config_path is set
        │                       (source_name keys reference the RAW, un-normalized column names)
        ▼
normalize_column_names()     -- only if normalize_column_names: true
        │                       (applies to whatever names remain, including any column
        │                        schema_config_path didn't cover)
        ▼
attach_technical_metadata()
        │
        ▼
apply_explode_columns() / apply_data_standardization_sql()
```

This is the exact order `notebooks/03_engine/03_lakeflow_declarative_pipeline.py` calls them
in. It matters because **every other `source_config`/`dq_config` field that names a column
(`explode_columns`, `data_standardization_sql`, `dq_config.rules[].expression`,
`record_id_column`, `target_config.primary_keys`, ...) must be written against the *final*
column names** — i.e. after both of these steps have run, not the source's original raw names.
If you use `schema_config_path` to rename `"CustomerID"` to `"customer_id"`, every other spec
field referencing that column must say `"customer_id"`, never `"CustomerID"`.

Using both together: `schema_config_path` runs first because its `source_name` keys are
written against the source's *original* raw column names (the ones you'd see by inspecting the
source directly) — running normalization first would change those names out from under it.
`normalize_column_names` then cleans up anything `schema_config_path` didn't explicitly cover
(a genuinely common case — you rarely need to hand-declare every single column, only the ones
that need a real type cast, rename, or comment).

## 1. `normalize_column_names`

```json
"source_config": {
  "path": "/Volumes/{{catalog}}/landing/customer/",
  "format": "csv",
  "normalize_column_names": true
}
```

Applies to every column: trim whitespace, lowercase, replace embedded whitespace and any
character that isn't `a-z`/`0-9`/`_` with an underscore, collapse repeated underscores, strip
leading/trailing underscores. `"  Customer ID#1  "` → `"customer_id_1"`.

Raises `FrameworkConfigError` if two source columns normalize to the same name (e.g.
`"Customer ID"` and `"customer_id"` both becoming `"customer_id"`) — rather than silently
dropping one via an implicit column overwrite. Rename one of the source columns, or use
`schema_config_path` for explicit, collision-free per-column control instead.

This is purely a rename — no type casting, no comments, no nullability. For that, use
`schema_config_path` below.

## 2. `schema_config_path`

```json
"source_config": {
  "path": "/Volumes/{{catalog}}/landing/customer/",
  "format": "csv",
  "schema_config_path": "/Volumes/{{catalog}}/landing/_schema_configs/customer/"
}
```

### How to author a schema_config file

A JSON or YAML file (same format-by-extension rule as an onboarding spec) with one top-level
`columns` array. See the complete worked example:
[`onboarding_templates/schema_config_example.json`](../onboarding_templates/schema_config_example.json)
/ [`.yaml`](../onboarding_templates/schema_config_example.yaml) (byte-equivalent pair, same
convention as the main onboarding template — see
[09_onboarding_yaml_json.md](09_onboarding_yaml_json.md)):

```json
{
  "columns": [
    {
      "source_name": "CustomerID",
      "target_name": "customer_id",
      "data_type": "STRING",
      "nullable": false,
      "comment": "Unique customer identifier from the source CRM system -- primary key."
    },
    {
      "source_name": "LifetimeValue",
      "target_name": "lifetime_value_usd",
      "data_type": "DECIMAL(12,2)",
      "nullable": true,
      "comment": "Cumulative customer lifetime value in USD, as computed by the source system."
    }
  ]
}
```

### Field reference

| Field | Type | Required | Description |
|---|---|---|---|
| `columns` | array of objects | **yes** | One entry per column you want to explicitly declare — you do **not** need to list every column; anything omitted passes through unchanged (original name, original type, no comment). |
| `columns[].source_name` | string | **yes** | The column's exact name as it arrives from the source (before any renaming) — case-sensitive, must match a real incoming column or `apply_schema_config` raises `FrameworkConfigError`. |
| `columns[].target_name` | string | no (defaults to `source_name`) | The output column name. Omit to cast/comment a column without renaming it. |
| `columns[].data_type` | string | no | Any Spark SQL type string (`"STRING"`, `"INT"`, `"BIGINT"`, `"DOUBLE"`, `"BOOLEAN"`, `"DATE"`, `"TIMESTAMP"`, `"DECIMAL(p,s)"`, ...) — passed directly to `Column.cast()`. Omit to leave the column's inferred type as-is. |
| `columns[].nullable` | boolean | no | **Documentation only — not enforced at ingestion time** (see below). Records intent for anyone reading the schema_config file. |
| `columns[].comment` | string | no | Attached as the output column's `metadata["comment"]` — the same schema-metadata key Delta/Unity Catalog read as a genuine column comment when the table is created, so it takes effect with no separate DDL step. Verify with `DESCRIBE TABLE EXTENDED <table>` after a real pipeline run. |

`{{catalog}}`/`{{env}}` placeholders inside the file's own content (not just the path — e.g.
inside a `comment` string) are substituted the same way the main onboarding spec is.

### Why `nullable` isn't enforced

Spark's `.cast()` cannot force a column to be non-nullable independent of the data actually
flowing through it — nullability is derived from the expression, not something you can declare
your way into. If you need genuine enforcement (fail or quarantine a row whose value is
actually null), add a companion `dq_config` rule referencing the **target** name:

```json
"dq_config": {
  "rules": [
    {"rule_id": "customer_id_not_null", "expression": "customer_id IS NOT NULL", "action": "fail"}
  ]
}
```

### `schema_config_path`: exact file vs. directory ("latest")

`schema_config_path` accepts either shape:

- **An exact file path** — used directly, no resolution needed.
- **A directory path** — resolved to the file *inside it* with the most recent modification
  time (`resolve_schema_config_path` in `ingestion/schema_config.py`), ties broken by the
  lexicographically-largest filename. This lets you drop a new schema version into a Volume
  directory (`schema_v1.json`, `schema_v2.json`, ...) without editing the onboarding spec's
  path on every change — the pipeline always picks up whichever file was written most
  recently the next time it runs.

```
/Volumes/{{catalog}}/landing/_schema_configs/customer/
├── schema_v1.json   (older modification time)
└── schema_v2.json   (newer modification time)  <-- this one is used
```

Concretely: `os.listdir()` the directory, keep only files (not subdirectories), sort by
`(modification_time, filename)`, take the last one. If the directory is empty, or the given
path doesn't exist as either a file or a directory, `FrameworkConfigError` is raised
immediately — a misconfigured path fails the pipeline update loudly rather than silently
reading nothing.

**Practical implication**: if you want a specific, pinned version regardless of what else gets
dropped into the directory later, point `schema_config_path` at the exact file, not the
directory. Use the directory form only when you deliberately want "always the newest" behavior.

### Structural validation

`onboarding/spec_validator.py` checks `schema_config_path` is a non-empty string at onboarding
time — it does **not** read or validate the referenced file's own content at onboarding time
(onboarding runs as a job task with no guaranteed Volume/workspace file access of its own to
the path a *pipeline* will later read from; the file is only ever actually loaded at pipeline
graph-definition time, inside `03_lakeflow_declarative_pipeline.py`). A malformed schema_config
file surfaces as a pipeline update failure (`FrameworkConfigError`), not an onboarding-time
error — check the pipeline's own update logs if `schema_config_path` looks right in the spec
but the pipeline still fails to start.

## Relevant files

* `src/flowx/lakeflow_framework/ingestion/column_normalization.py` —
  `normalize_column_name` (pure), `normalize_column_names` (DataFrame).
* `src/flowx/lakeflow_framework/ingestion/schema_config.py` —
  `resolve_schema_config_path`, `load_schema_config`, `apply_schema_config`.
* `src/flowx/lakeflow_framework/onboarding/spec_validator.py` —
  `_validate_ingestion_source_config`'s `normalize_column_names`/`schema_config_path` checks.
* `onboarding_templates/schema_config_example.json` / `.yaml` — the worked example above, in
  full.
* `notebooks/03_engine/03_lakeflow_declarative_pipeline.py` — the exact call order.
* `tests/unit/test_column_normalization.py`, `tests/unit/test_schema_config.py` — including
  the DataFrame-transform tests (live `spark` fixture, no table created — same convention as
  `tests/unit/test_column_ordering.py`).
