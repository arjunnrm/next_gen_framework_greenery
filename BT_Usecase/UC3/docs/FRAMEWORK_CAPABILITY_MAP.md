# FlowX Framework Capability Map — UC3 Excalibur Build

Phase A deliverable. Every row below was resolved by reading the framework's own source, not by
recall. The authority for allowed attributes is
`src/flowx/lakeflow_framework/onboarding/spec_validator.py` (the `ALLOWED_*` / `REMOVED_*`
constants), which `tests/unit/test_unknown_key_rejection.py` asserts against the JSON schema.

**Framework version in repo:** FlowX (`src/flowx/lakeflow_framework`), spec v2 schema, v1.7.3-era
(Single-Read DAG mandate present; `materialize: "never"` rejected).

> **Unknown attributes are a hard error since v1.7.1.** Any key not in the allowlists below is
> rejected at onboarding — not ignored. So "it looks plausible" is worth nothing; every spec must
> pass `agent_tools.validate_json` before it is handed over.

---

## 1. The eight requested capabilities

| # | Capability | Verdict | FlowX config key | Source of truth |
|---|---|---|---|---|
| 1 | **SCD type selection** | **Config** (pre-confirmed) | `target_config.cdc_load_strategy` = `SCD1` \| `SCD2` (+ `primary_keys`, `sequence_by_column`) | `spec_validator.py:83` `ALLOWED_INGESTION_CDC_STRATEGIES` |
| 2 | **Hashing** | **Config** (boolean gate; column set is derived) | `target_config.generate_hash_columns` (bool, default `True` for CDC strategies) + `columns_to_check` / `columns_to_exclude` to shape the hashed set | `cdc/hashing.py`, `cdc/comparison_columns.py` |
| 3 | **Null standardization** | **Config** | `source_config.data_standardization_sql` — a list of single column expressions | `spec_validator.py:646` `_validate_data_standardization_sql` |
| 4 | **Column/table tagging** | **Config** (pre-confirmed) | `governance_tags.table_tags` (map) + `governance_tags.column_tags[]` (`{column, tags{}}`) | `governance/tags.py`, `spec_validator.py:297` |
| 5 | **Liquid clustering** | **Config**, but **capped at 3 columns** | `target_config.liquid_clustering_columns` | `spec_validator.py:484` `MAX_LIQUID_CLUSTERING_COLUMNS = 3` |
| 6 | **Iceberg / UniForm** | **Config**, but framework emits **one** property, not three | `target_config.table_properties.enable_iceberg_read_uniformity: true` | `storage/table_properties.py:119` |
| 7 | **Batch load** | **Config** | `target_type: "batch_table"`; batch CSVs land via `source_type: "autoloader"` | `spec_validator.py:75` |
| 8 | **Batch-vs-stream reconciliation** | **Config** — a first-class engine | `reconciliation_flows[]` with `match_keys`, `compare_columns`, `execution_mode`, `logging_config`, `target_configs[].append_target_table` | `reconciliation/` package, `spec_validator.py:310` |

**Conclusion: zero hand-written code is required for Jobs 2 and 3.** All eight capabilities exist
as configuration. The only hand-written artefact in this build is Job 1 (the streaming simulator),
which is legitimately custom per §0, plus the test-data generator.

---

## 2. Four deviations between the prompt's spec and the framework's actual behaviour

These are the places where the request cannot be met literally. In each case the framework's
behaviour is the better engineering choice and is what this build follows — but they are called
out rather than silently absorbed.

### 2.1 Hash encoding is **hex, not base64** (§5.1)

The prompt asks for "SHA-256, base64-encoded". The framework's single canonical construction is:

```
sha2(concat_ws('||', coalesce(trim(lower(cast(c AS STRING))), '__NULL__'), ...), 256)
```

producing a **64-character lowercase hex** digest. This is deliberately the *only* implementation
in the codebase (`cdc/hashing.py`) — three copies previously drifted apart and silently broke
reconciliation, so the module docstring explicitly forbids rebuilding it locally. There is no
config key to switch encoding.

**Decision: accept hex.** Base64 would mean hand-writing a second hash construction, which is
exactly the class of duplication §0 prohibits and which this framework has already been burned
by. The reconciliation join is hash-to-hash within one framework, so the encoding is internal.

### 2.2 The hash column set is **derived, not enumerated** (§5.1)

The prompt asks to "list the exact ordered column list feeding the hash explicitly in the JSON".
FlowX resolves it at runtime instead, via `resolve_comparison_columns(all_columns, primary_keys,
columns_to_check, columns_to_exclude)`, which:

- **excludes `primary_keys` unconditionally** — so §5.1's "excluding the primary key columns" is
  already guaranteed by construction, not by our config;
- excludes `FRAMEWORK_TECHNICAL_COLUMNS`;
- returns the result **alphabetically sorted** for determinism.

So the prompt's requirement is satisfied *more* strongly than asked — the legacy Ab Initio bug
(PKs leaking into the hash) is structurally impossible here. The two timestamps are excluded via
config: `columns_to_exclude: ["sys_creation_date", "sys_update_date"]`.

To keep the audit trail the prompt wants, the intended set is documented in UC3_MASTER_DOCUMENT.md section 10.2.
(The specs carry no comment keys beyond a single `_about` header, by the user's decision of 2026-09-07 -- BUILD_CONTRACT.md section 17.10.)

### 2.3 Liquid clustering is **capped at 3 columns**; two PKs exceed it (§2.3)

`MAX_LIQUID_CLUSTERING_COLUMNS = 3`. But:

| Table | PK | PK width | Fits? |
|---|---|---|---|
| `CUSTOMER` | `customer_id` | 1 | yes |
| `SUBSCRIBER` | `subscriber_no, customer_id` | 2 | yes |
| `PHYSICAL_DEVICE` | `customer_id, subscriber_no, equipment_no, phy_seq_no` | **4** | **no** |

**Decision for `PHYSICAL_DEVICE`:** cluster on `__framework_hash_key` — the SHA-256 of the ordered
PK columns, which the framework already materializes for every CDC-dispatched flow. This is the
framework's own documented recommendation (`cdc/hashing.py`: *"especially once the target table is
liquid-clustered on `__framework_hash_key`"*), gives full-PK locality in one column, and is what
the reconciliation join actually probes on. Recorded in `BUILD_CONTRACT.md` as binding.

### 2.4 UniForm emits **one** property, not the three requested (§2.4)

The prompt lists `delta.columnMapping.mode`, `delta.enableIcebergCompatV2` and
`delta.universalFormat.enabledFormats`. `build_table_properties` sets only
`delta.universalFormat.enabledFormats = "iceberg"` from
`table_properties.enable_iceberg_read_uniformity`. The other two are **prerequisites Databricks
sets automatically** when UniForm is enabled on a Lakeflow-managed table. `table_properties` is a
closed key set — arbitrary `delta.*` passthrough is not accepted, so they cannot be forced.

**Decision: set the framework key.** Verify the resulting properties on the live table during
Phase C (`DESCRIBE DETAIL` / `SHOW TBLPROPERTIES`) rather than asserting them from the spec.

---

## 3. Governance CSVs — RESOLVED (no longer a blocker)

The three sheets are present at `docs/UC3/{PHYSICAL_DEVICE,CUSTOMER,SUBSCRIBER}_DDL.csv` and were
parsed and verified. **Every governance fact stated in the prompt's §1 is confirmed by the sheets
— PKs, Drop lists and Null lists all match verbatim, with no discrepancy to report.**

Business-column counts also match §1 exactly (31 / 90 / 133), once rows are split on whether
`Feed column name` is populated: populated ⇒ a source column to ingest; empty ⇒ a target-only
column (the 6-column audit envelope: `hash_value`, `src_deleted_flg`, and four legacy `gcp_*`
fields).

Full parsing rules, header quirks (`Sesnitive Columns` is misspelled in the source;
`PHYSICAL_DEVICE` spells the tier flag `csql.securedro`), the audit-column disposition and the
Oracle→Spark type mapping are in [`BUILD_CONTRACT.md`](BUILD_CONTRACT.md) §6.

One corroboration worth noting: the sheets' own description of `hash_value` — *"Generated with
combination of columns expect key, timestamp"* — independently confirms §5.1's hash rule and
matches what `resolve_comparison_columns` already does by construction, reinforcing §2.2's finding
that the hash needs no custom code.

## 4. Attribute-name traps for this build

Names that would be silently wrong in another framework and are **hard-rejected** here:

| Do not write | Write instead |
|---|---|
| `scd_type: 2` | `target_config.cdc_load_strategy: "SCD2"` |
| `cdc_config: {...}` | CDC fields live directly in `target_config` |
| `sequence_by` | `target_config.sequence_by_column` |
| `primary_key` (scalar) | `target_config.primary_keys` (list) |
| `tags` | `governance_tags.table_tags` |
| `cluster_by` | `target_config.liquid_clustering_columns` |
| `null_standardization` | `source_config.data_standardization_sql` |
| `hash: {...}` | `target_config.generate_hash_columns` (bool) |
| `columns[]` (with per-column types) | **no such key** — see §5 below |
| `source_object` | `source_config.source_table` (zerobus) / `.path` (autoloader) |
| `target_schema: "uc3.bronze"` | `target_catalog: "uc3"` + `target_schema: "bronze"` (separate keys) |

### 5. The `columns[]` key does not exist

§5.1 asks for a `columns[]` array carrying `name` / `source_type` / `target_type` / `nullable` /
`comment` / `tags` per column. **FlowX has no such attribute** — `ALLOWED_INGESTION_FLOW_KEYS`
admits no `columns` key, and schema is inferred from the source (or pinned via
`source_config.schema_config_path`). The per-column concerns decompose across three real keys:

| §5.1 wanted | FlowX equivalent |
|---|---|
| `columns[].tags` | `governance_tags.column_tags[]` |
| `columns[].comment` | `source_config.schema_config_path` → a schema-config document |
| `columns[].name/type/nullable` | `source_config.schema_config_path`, or inferred |
| the `Drop(DF)=Y` changelog | UC3_MASTER_DOCUMENT.md section 11.2 (not carried in the spec) |

`onboarding_templates/schema_config_example.json` is the shape for the schema-config document.
This is a genuine structural difference from the prompt's assumed schema, not a naming quibble.

---

## 6. Validation command (run before handing over any spec)

```bash
python -c "
import json,sys; sys.path.insert(0,'src')
from flowx.lakeflow_framework.onboarding.agent_tools import validate_json
r = validate_json(open('SPEC.json', encoding='utf-8').read())
print(r['summary'])
[print(' ERROR:', e) for e in r['errors']]
[print(' warn :', w) for w in r['warnings']]
"
```

Offline, no cluster, sub-second. Loop until `valid` is `True`.
