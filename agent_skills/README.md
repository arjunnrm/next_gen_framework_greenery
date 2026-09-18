# 🤖 Metaflow AI Agent Skill & Tool Catalog

> **Purpose**: Single authoritative registry summarizing all AI Agent Skills, tool specifications, input/output schemas, error-handling behaviors, and integration patterns for the Metaflow.

---

## 📋 Executive Skill & Tool Catalog Summary

| Skill / Tool Name | Category | Primary Function | Input Schema | Return / Output Schema | Error Handling Behavior | Integration Patterns |
|---|---|---|---|---|---|---|
| **`validate_json`** | Tool | Lint & validate candidate onboarding specs immediately upon generation. | `spec_content` (str), `catalog` (str), `env` (str), `strict_mode` (bool) | `{"valid": bool, "error_count": int, "errors": list, "warnings": list, "summary": str}` | Returns complete non-failing diagnostic list `["<json_path>: <message>"]`. | OpenAI Function Calling, LangChain `StructuredTool`, Semantic Kernel |
| **`onboard_entity`** | Tool | Idempotent lifecycle create/update execution for pipelines & control tables. | `spec_content` (str), `action_type` ('create'/'update'), `catalog`, `environment`, `auto_deploy`, `dry_run` | `{"success": bool, "dataflow_group_id": str, "action_performed": str, "affected_tables": list, "audit_log_id": str, "message": str}` | Pre-validates spec; rejects invalid specs with `success=False` before database mutation. | LangChain Agent, Databricks Genie, Mosaic AI |
| **`get_catalog_schema_parameters`** | Tool | Introspect Unity Catalog schemas, Delta tables, Volume paths, and tags. | `catalog_name` (str), `schema_name` (str), `table_name` (optional str), `include_column_metadata` (bool), `include_governance_tags` (bool) | `{"catalog": str, "schema": str, "table_count": int, "tables": [{"table_name": str, "columns": [...], "tags": {...}}]}` | Catches catalog lookup exceptions; returns safe default/empty lists with error diagnostics. | OpenAI Function Calling, UC Python Tools |
| **`validate_observability_config`** | Tool | Lint and validate `{"observability": [...]}` telemetry fragments. | `config_text` (str), `catalog` (str), `env` (str) | `{"valid": bool, "errors": list, "summary": str}` | Validates against `ALLOWED_OBSERVABILITY_DESTINATION_TYPES`, returns line-level errors. | OpenAI Tools API, LangChain `StructuredTool` |
| **`generate_pipeline_onboarding_config`** | Tool | Auto-generate `observability[]` array from minimal parameters. | `dataflow_group_id` (str), `destination_targets` (list), `service_name`, `deployment_environment` | `{"observability": [ { "destination_id": str, "destination_type": str, ... } ]}` | Validates destination templates ('databricks_volume', 'otlp_http'); raises `ValueError` on bad inputs. | Prompt chaining, automated spec synthesis |
| **`diagnose_pipeline_telemetry_failures`** | Tool | Diagnose failed telemetry task runs from traceback/error messages. | `error_message` (str) | `{"matched": bool, "category": str, "likely_cause": str, "remediation": str}` | Matches regex patterns against Error Handling Matrix; returns `null` category on unknown errors. | SRE Troubleshooting Assistant, Incident Copilot |
| **`flowx-governance`** | Skill | Enforces standardized naming (`dfg_*`, `df_*`, `tf_*`), mandatory tagging, and OTel metric naming. | Contextual guidelines and regex validation rules. | Evaluates naming conformance and tags completeness. | Flags warnings during AST validation without breaking pipeline compilation. | Agent System Prompt / Skill Directive |

---

## 🚦 The non-negotiable loop for a generating agent

Since **v1.7.1** the framework **rejects any attribute it does not read** — an unrecognised key
is a hard validation error, not a silent no-op. (Previously `data_quality` instead of `dq_config`
onboarded cleanly and ran the pipeline without ever executing a single DQ rule.) A generated spec
that merely *looks* right is therefore not evidence of anything.

1. **Copy**, don't compose. Start from the closest of the five machine-validated specs in
   [`reference/golden_specs.json`](reference/golden_specs.json) — minimal autoloader, SCD2 + DQ +
   tags, transformation join, zerobus SCD1, in-pipeline reconciliation. Each is asserted valid by
   `tests/unit/test_golden_specs.py`, so they cannot rot.
2. **Validate** with `validate_json` — offline, no Spark, no cluster, sub-second.
3. **Fix from the message.** Errors name the correct attribute (`Use dq_config.`,
   `Use target_config.partition_columns.`) via the validator's `UNKNOWN_KEY_ALIASES`. Repeat.
4. **Show** the user the spec *and* the passing validation result.

```python
import sys; sys.path.insert(0, "src")
from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

result = validate_json(open("my_spec.json", encoding="utf-8").read())
print(result["summary"])
for error in result["errors"]:
    print(" ERROR:", error)
```

Keys starting with `_` are always allowed as author comments (JSON has no comment syntax), as is
`$schema`. The discoverable Claude Code skill wrapping all of this lives at
`.claude/skills/flowx-onboarding/`; re-sync its reference copies with
`python scripts/sync_agent_skill.py` after changing anything here.

---

## 🛠️ Industry-Standard Integration Patterns

### 1. LangChain / LangGraph Integration
```python
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field
from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

class ValidateJsonInput(BaseModel):
    spec_content: str = Field(description="Raw JSON/YAML onboarding specification string")
    catalog: str = Field(default="poc", description="Target Unity Catalog catalog")
    env: str = Field(default="dev", description="Target environment")

validate_json_tool = StructuredTool.from_function(
    func=validate_json,
    name="validate_json",
    description="Validates candidate Metaflow onboarding specs immediately upon generation",
    args_schema=ValidateJsonInput,
)
```

### 2. Semantic Kernel Integration
```csharp
// Semantic Kernel KernelFunction Registration
[KernelFunction, Description("Validates candidate Metaflow onboarding specification")]
public static string ValidateJson(
    [Description("Raw onboarding JSON/YAML string")] string spec_content,
    [Description("Target catalog")] string catalog = "poc",
    [Description("Target environment")] string env = "dev"
)
```

### 3. OpenAI Assistants / Chat Completions Tools Array
Directly import `agent_skills/tool_specifications.json` and pass `tools` into `openai.chat.completions.create(tools=tool_specifications["tools"], ...)`.
