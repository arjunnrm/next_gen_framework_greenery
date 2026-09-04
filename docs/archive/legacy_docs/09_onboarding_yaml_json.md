# Onboarding: YAML and JSON Format Parity

> See also: [Documentation index](README.md).

## Purpose

Let FlowX onboarding specs be authored in either JSON or YAML, with identical
validation, upsert, and audit behavior -- format is a authoring-convenience choice, not a
different code path.

## Responsibilities

`onboarding/spec_loader.py::load_and_template_spec` picks a parser by file extension
(`.yaml`/`.yml` -> `yaml.safe_load`, anything else -> `json.loads`) after `{{catalog}}`/
`{{env}}` template substitution. Both parsers produce the exact same in-memory `dict`
shape -- `spec_validator.validate_spec`, `metadata_upsert.py`, and `audit_logger.py` never
branch on format.

## Inputs

* `onboarding_templates/pipeline_onboarding_template.json` and `.yaml` -- the standard
  onboarding template (the "kitchen sink"), covering every field category from
  source/target config through SCD/CDC, encryption, DQ, governance, and reconciliation. See
  [17_onboarding_template_reference.md](17_onboarding_template_reference.md) for a
  field-by-field walkthrough of every attribute in this file. **This is the actual
  byte-for-byte JSON/YAML equivalent pair** that
  `tests/unit/test_spec_loader.py::test_json_and_yaml_templates_parse_to_the_same_structure`
  asserts parse to `==` in-memory dicts -- see the worked side-by-side excerpt below.
* Any `test_specs/*.json` file -- these are real, engine-validated specs (grep for the
  topic you care about, e.g. `source_type` or `cdc_load_strategy`, to find a concrete worked
  example). None of them currently has a `.yaml` twin on disk -- JSON/YAML parity is
  exercised by the `onboarding_templates/` pair above and by
  `tests/unit/test_spec_loader.py`'s other cases (templating, `spec_version` hashing,
  malformed-file handling), not by a per-spec duplicate file. Nothing stops a team from
  authoring (or converting) any of these as YAML -- the loader doesn't care.

## Worked example: the same flow in both formats

Both parsers must land on identical field names and shapes -- there's no format-specific
schema. Below is one real ingestion flow from `onboarding_templates/pipeline_onboarding_template.{json,yaml}`
(the `df_template_ingest` flow), JSON first, then the equivalent YAML. Every field name here
matches the schema documented in
[01_control_metadata_schema.md](01_control_metadata_schema.md).

```json
{
  "dataflow_id": "df_template_ingest",
  "source_type": "autoloader",
  "target_catalog": "{{catalog}}",
  "target_schema": "bronze_example",
  "target_table": "example_raw",
  "target_type": "streaming_table",
  "source_config": {
    "path": "/Volumes/{{catalog}}/landing/example_raw_zone/incoming/",
    "format": "csv",
    "file_pattern": "orc_*",
    "schema_location": "/Volumes/{{catalog}}/landing/_schemas/example_raw/",
    "data_standardization_sql": ["trim(region) AS region"],
    "capture_technical_metadata": true
  },
  "target_config": {
    "cdc_load_strategy": "APPEND",
    "storage_format": "delta",
    "partition_columns": ["region"],
    "encrypted_columns": [
      {
        "column_name": "pii_column",
        "output_column": "pii_column",
        "mode": "GCM",
        "secret": {"secret_catalog": "{{catalog}}", "secret_schema": "security", "secret_key": "pii_encryption_key"}
      }
    ]
  },
  "dq_config": {
    "rules": [
      {"rule_id": "dq_amount_non_negative", "expression": "amount >= 0", "action": "quarantine"}
    ],
    "quarantine_table": "example_raw_quarantine",
    "record_id_column": "example_id"
  },
  "governance_tags": {
    "column_tags": [
      {"column": "pii_column", "tags": {"mask": "PII", "classification": "restricted"}}
    ],
    "table_tags": {"row_filter": "region_restricted", "domain": "example"}
  }
}
```

```yaml
dataflow_id: df_template_ingest
source_type: autoloader
target_catalog: '{{catalog}}'
target_schema: bronze_example
target_table: example_raw
target_type: streaming_table
source_config:
  path: /Volumes/{{catalog}}/landing/example_raw_zone/incoming/
  format: csv
  file_pattern: orc_*
  schema_location: /Volumes/{{catalog}}/landing/_schemas/example_raw/
  data_standardization_sql:
  - trim(region) AS region
  capture_technical_metadata: true
target_config:
  cdc_load_strategy: APPEND
  storage_format: delta
  partition_columns:
  - region
  encrypted_columns:
  - column_name: pii_column
    output_column: pii_column
    mode: GCM
    secret:
      secret_catalog: '{{catalog}}'
      secret_schema: security
      secret_key: pii_encryption_key
dq_config:
  rules:
  - rule_id: dq_amount_non_negative
    expression: amount >= 0
    action: quarantine
  quarantine_table: example_raw_quarantine
  record_id_column: example_id
governance_tags:
  column_tags:
  - column: pii_column
    tags:
      mask: PII
      classification: restricted
  table_tags:
    row_filter: region_restricted
    domain: example
```

`yaml.safe_load` on the second block and `json.loads` on the first produce the same Python
`dict` (modulo key order, which Python dicts don't care about) -- that equivalence is exactly
what `test_json_and_yaml_templates_parse_to_the_same_structure` checks across the *entire*
template file, not just this one flow.

A second real excerpt worth calling out -- the `df_template_zerobus_ingest` flow's
`target_config`, showing every CDC-related field living directly inside `target_config`
with **no separate `cdc_config` sibling**:

```json
"target_config": {
  "cdc_load_strategy": "SCD1",
  "primary_keys": ["event_id"],
  "cdc_operation_column": "op",
  "cdc_operation_mapping": {"delete_values": ["D"]},
  "columns_to_exclude": ["batch_load_ts", "source_extract_filename"],
  "generate_hash_columns": true
}
```

## Outputs

An in-memory spec `dict`, the templated raw text (for the audit trail's
`raw_spec_payload`), and a deterministic `spec_version` hash of that text -- identical
regardless of source format.

## Configuration

Nothing to configure -- the format is inferred from the file's own extension. Use
whichever your team prefers; mix and match freely (a `dfg_x` spec in YAML today can be
resubmitted as JSON tomorrow -- they upsert into the same control-table rows by
`dataflow_group_id`/`dataflow_id`/`flow_step_id`, not by file identity).

## Main execution flow

1. `read_raw_spec_text` -- plain `open()`, falling back to `dbutils.fs.head`.
2. `substitute_environment_placeholders` -- `{{catalog}}`/`{{env}}` string substitution,
   format-agnostic (operates on raw text before parsing).
3. Extension-based dispatch to `yaml.safe_load` or `json.loads`.
4. A parsed non-`dict` top level (e.g. a bare YAML list) is rejected immediately with an
   `OnboardingValidationError` naming the actual type -- fails fast rather than letting a
   confusing `AttributeError` surface deeper in validation.

## Design decisions

* **`yaml.safe_load`, never `yaml.load`** -- onboarding specs are configuration, not
  trusted code; `safe_load` refuses arbitrary Python object construction from the YAML
  stream. See [PyYAML security notes](https://pyyaml.org/wiki/PyYAMLDocumentation).
* Parser dispatch is by **file extension**, not content sniffing -- unambiguous, and
  every valid JSON document also happens to be parseable by many YAML parsers (JSON is
  a YAML subset in practice), which would make content-sniffing ambiguous rather than
  more convenient.

## Error handling

`OnboardingValidationError` for both a `json.JSONDecodeError` and a `yaml.YAMLError`,
each naming the file, the detected format, and the underlying parser error.

## Extension points

A third format (e.g. TOML) would add one more `elif` branch and one more exception type
to the `except` tuple in `load_and_template_spec` -- the rest of the onboarding pipeline
is unaffected.

## Example usage

`resources/onboarding_job.yml` is the reusable job that takes `spec_file_path` as a genuine
per-run job parameter (see [05_deployment_guide.md](05_deployment_guide.md) for the full
mechanics) -- it doesn't care whether the file is JSON or YAML, only its extension:

```bash
# A JSON spec
databricks bundle run onboarding_job --target dev \
  --params spec_file_path=/Workspace/.../test_specs/spec_07_volume_scd1_scd2_customer_360.json,catalog=poc,env=dev,action_type=CREATE

# A YAML spec -- identical parameters, only the extension (and therefore the parser) differs
databricks bundle run onboarding_job --target dev \
  --params spec_file_path=/Workspace/.../onboarding_templates/pipeline_onboarding_template.yaml,catalog=poc,env=dev,action_type=CREATE
```

## Relevant tests

`tests/unit/test_spec_loader.py` -- JSON/YAML structural equivalence, templating,
deterministic + change-sensitive `spec_version` hashing, malformed-JSON and
malformed-YAML error paths, non-object top-level rejection, missing-file handling.
