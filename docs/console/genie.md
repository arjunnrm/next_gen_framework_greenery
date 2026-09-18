# :material-chat-question: Genie space · conversational diagnostics

**The `Metaflow Framework Observability` Genie space answers plain-language questions about what a dataflow group is supposed to do and how it is actually behaving, by reading the same eleven views the dashboard reads.** Source: `databricks-genie/flowx_observability.geniespace.json`, deployed by `resources/flowx_genie/flowx_observability_genie_space.yml`.

!!! abstract "Quick links"
    - Why the views and only two raw system tables: [docs/17 §7](../17_framework_observability_and_genie.md#7-the-genie-space)
    - The batch counterpart that leaves an artefact: the [documentation job](../17_framework_observability_and_genie.md#8-the-documentation-generator)
    - The pillar: [Pillar 4 · Observability](../pillars/observability.md)

## What it can see

Fifteen data sources: the eleven `observability.v_*` views, two control tables for row-level detail, and two raw billing tables that only `AI_FORECAST` needs.

| Group | Tables | Answers |
|---|---|---|
| Declarative (what was onboarded) | `v_dataflow_group_catalog`, `v_flow_inventory`, `v_pipeline_registry` | "What does group X do, flow by flow?" |
| Operational (how it runs) | `v_pipeline_updates`, `v_job_runs`, `v_flow_metrics`, `v_dq_results`, `v_reconciliation_health`, `v_dataflow_lineage`, `v_group_health_summary`, `v_dataflow_cost` | "Which groups are unhealthy and why?", "What did each group cost?" |
| Row-level detail | `config.onboarding_audit_log`, `config.reconciliation_mismatch_log` | "Who onboarded this?", "What exactly did not match?" |
| Forecast inputs | `system.billing.usage`, `system.billing.list_prices` | "Forecast our spend for the next two days" |

The single instruction block teaches Genie the vocabulary (dataflow group, the three flow kinds, load strategies), the join key, the deduplication rules and the terminal-status ordering, so it reads answers off the views instead of rediscovering the traps on every question.

## Sample questions that ship with the space

Ask these first; each maps to a curated example SQL pair so the answer is deterministic.

1. Show me the details of every dataflow group and what features they use
2. Explain what `dfg_uc6_ea_flood_warning` does, flow by flow
3. Which dataflow groups are unhealthy, and why?
4. Document `dfg_uc7_cdr_asn` as a design document for a new engineer
5. Which data quality rules are failing and on which datasets?
6. How many rows did each dataflow group process in the last 30 days?
7. What did each dataflow group cost to run, and which is least efficient per row?
8. Show reconciliation match rates and anything that is not matching
9. Which pipeline updates failed, and were they retried?
10. What is the real lineage from Bronze to Gold for `dfg_uc6_ea_flood_warning`?
11. Forecast our framework spend for the next two days
12. What is our projected monthly run rate at the current spend trend?

## Asking well

=== "Good"

    - Name the dataflow group: `dfg_…` is the primary key of everything.
    - Ask for one grain: "per flow", "per update", "per day".
    - Ask "why" after "which": Genie will pull `v_group_health_summary` first, then the detail view.

=== "Weak"

    - "Show me the logs" — there are no logs here; ask for updates, DQ results or mismatches.
    - "Executor memory for the last run" — Spark-level telemetry is not in the views; use the pipeline UI.
    - A question that names a table Genie cannot see (a Bronze table): it can describe the flow, not read the data.

## Diagnostics you can do from a chat

| Symptom | Ask | Then |
|---|---|---|
| A group shows as unhealthy | "Why is `dfg_x` unhealthy?" | Genie returns the failing dimension (updates, DQ, recon, cost); open the matching [dashboard page](observability_dashboard.md). |
| Rows stopped arriving | "How many rows did `dfg_x` write per day this week?" | If zero, check the landing path and the ZIP `target_volume_path` trap. |
| Reconciliation drift | "Show reconciliation health: what is not matching?" then "Show the mismatch log rows for `rf_…`" | The second question reads `config.reconciliation_mismatch_log`. |
| Cost spike | "Which group's cost rose the most this week, and did its row count rise too?" | Cost vs rows is the efficiency question; a rise without rows is a retry storm or a full refresh. |

## Operating the space

Three format rules are **API-enforced** on the serialized JSON:

1. `version: 2` at the top level.
2. Every human-readable string is an **array of lines**, each ending in `\n` except the last.
3. `data_sources.tables` sorted by identifier; every other repeated block sorted by `id`. An unsorted array is rejected at deploy, not normalised.

```bash
# UI edits back into the repo (re-sorted, canonical), then review the diff
databricks bundle generate genie-space --resource flowx_observability_genie_space --force

# Another catalog? The space carries fully-qualified names, so render a per-target copy
python scripts/render_genie_space.py --catalog <catalog>
```

!!! warning "The catalog is baked in"
    Unlike the dashboards, a Genie space has no `dataset_catalog`. The canonical file names the `flowx` catalog; a target on another catalog needs its own rendering (`databricks-genie/flowx_observability.<catalog>.geniespace.json`) and a resource pointing at it. Deploying the `flowx` rendering to a workspace without that catalog fails the whole `bundle deploy`, which is why some targets deploy with `--select` while the space is fixed.

## Related

- [Observability dashboard](observability_dashboard.md) · [Control metadata dashboard](control_dashboard.md) · [Agent skills & tools](agent_skills.md)
- [docs/17 §7 · The Genie space](../17_framework_observability_and_genie.md#7-the-genie-space)
