<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# `dq`

Data-quality expectations and quarantine routing.


2 modules.


## `lakeflow_framework/dq/expectations.py`

Native Lakeflow DQ expectations: warn / drop / fail, applied as a decorator.


### Functions

| Signature | Purpose |
|---|---|
| `apply_dq_expectations(dq_rules: List[Dict[str, Any]])` | Decorator factory applying native ``warn`` / ``drop`` / ``fail`` DQ expectations. |


## `lakeflow_framework/dq/quarantine.py`

Dynamic quarantine routing: derived flag/rule-id columns plus a main+quarantine table pair.


### Functions

| Signature | Purpose |
|---|---|
| `add_quarantine_columns(df: DataFrame, dq_rules: List[Dict[str, Any]], pipeline_run_id: Optional[str] = None, record_id_column: Optional[str] = None) -> DataFrame` | Attach quarantine routing + diagnostic metadata for ``action: quarantine`` rules. |
| `register_main_and_quarantine_tables(base_view_name: str, target_table: str, target_catalog: str, target_schema: str, target_config: Dict[str, Any], dq_rules: List[Dict[str, Any]], comment: Optional[str], is_streaming: bool, needs_cdc_dispatch: bool, quarantine_table_override: Optional[str] = None, flow_label: Optional[str] = None) -> str` | Register the quarantine-filtered "clean" dataset, plus its sibling quarantine table if configured. |

