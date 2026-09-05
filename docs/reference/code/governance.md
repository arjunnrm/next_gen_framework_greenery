<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# `governance`

Unity Catalog tagging applied after deployment.


1 modules.


## `lakeflow_framework/governance/tags.py`

Unity Catalog governance: tags-only model (v2 schema, replaces governance/abac.py).


### Functions

| Signature | Purpose |
|---|---|
| `apply_governance_tags(spark: SparkSession, catalog: str, schema: str, table: str, governance_tags: Dict[str, Any]) -> None` | Apply every configured column tag and table tag to a target Delta table. |
| `apply_all_governance_tags(spark: SparkSession, control_catalog: str, group_id: str) -> None` | Apply governance tags (column + table) for every active flow in ``group_id``. |

