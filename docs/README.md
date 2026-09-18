# 📚 Metaflow — Production Documentation Suite

Welcome to the **Metaflow** unified documentation suite. This production-ready, domain-modular documentation serves as the single source of truth for developers, data architects, project managers, and AI coding agents.

---

## 🗺️ Documentation Directory & Domain Modules

| Module | Document | Target Audience | Purpose & Key Highlights |
|---|---|---|---|
| **00** | [**📖 Master Reference Index**](00_master_reference_index.md) | **All Stakeholders & Agents** | **Searchable Technical Lookup Dictionary.**<br>• Every JSON/YAML attribute indexed with Type, Required, Default, and Allowed Values.<br>• Master cross-reference dictionary for CDC strategies, engine features, and framework columns.<br>• Verified official Databricks documentation URLs. |
| **01** | [**🏗️ Platform Architecture & Core Concepts**](01_platform_architecture.md) | **Architects & Engineers** | **Foundational Design & Execution Model.**<br>• Two-phase execution model (Graph Definition vs. Graph Execution).<br>• Medallion architecture alignment & control metadata ERD (8 control tables).<br>• Decoupled Lakeflow native DAG compilation (`@dlt.table`, `@dlt.view`, `dlt.create_sink`). |
| **02** | [**📥 Ingestion & Source Reader Engine**](02_ingestion_and_sources.md) | **Pipeline Developers** | **Bronze Layer Source Handling.**<br>• Auto Loader, Zerobus streaming, and ASN.1 BER/DER decoding (`mapInPandas`).<br>• PGP/ZIP pre-extraction decryption and archive handling.<br>• Column normalization, technical metadata injection, and external `schema_config_path` resolution. |
| **03** | [**🔄 Transformation & CDC Engine**](03_transformation_and_cdc.md) | **Pipeline Developers** | **Silver/Gold Layer CDC & Processing.**<br>• Exhaustive CDC strategies (`APPEND`, `TRUNCATE_AND_LOAD`, `SCD1`, `SCD2`, `SCD3`, `FULL_SNAPSHOT_CDC`).<br>• Deterministic hash columns (`__framework_hash_key`, `__framework_hash_value`).<br>• v1.4.0: `FULL_SNAPSHOT_CDC_NO_PK` and the surrogate-key engine removed (§2.7).<br>• Runtime dynamic parameter substitution (`${param}`, `{{catalog}}`, `{{env}}`). |
| **04** | [**🛡️ Data Quality & Governance**](04_data_quality_and_governance.md) | **Governance Leads & Developers** | **Data Integrity & Unity Catalog Security.**<br>• DQ Expectations (`warn`, `drop`, `fail`, `quarantine`) and automated quarantine table routing.<br>• Diagnostic quarantine metadata (`__framework_quarantine_timestamp_utc`, failed rule array).<br>• Unity Catalog Table & Column tagging and Attribute-Based Access Control (ABAC). |
| **05** | [**🔐 Security & Cryptography**](05_security_and_cryptography.md) | **Security Engineers & Architects** | **PII Protection & Secret Management.**<br>• Column-level AES encryption (`GCM`, `CBC`, `ECB`) and PGP digital signatures.<br>• Unity Catalog 3-level secret paths (`secret:<scope>:<key>`).<br>• Deterministic encryption verification, key rotation runbooks, and zero-key-leakage architecture. |
| **06** | [**📤 Egress & Lakeflow Sinks**](06_egress_and_lakeflow_sinks.md) | **Integration Engineers** | **Real-Time & Batch Data Egress.**<br>• Native in-graph Lakeflow sinks (`dlt.create_sink` + `@dlt.append_flow`).<br>• Sink formats: `delta`, `kafka`, and `pgp_zip` direct Volume export.<br>• Eager secret resolution at graph-definition time. |
| **07** | [**⚖️ Reconciliation & Self-Healing**](07_reconciliation_engine.md) | **Data Stewards & Operators** | **Cross-Dataset Consistency Verification.**<br>• Source vs. multi-target comparison, hash-first diffing, and drift classification (`VALUE_DRIFT`, `MISSING_IN_TARGET`).<br>• Per-record mismatch audit logging.<br>• Idempotent and streaming self-healing append workflows. |
| **08** | [**📊 Observability & OpenTelemetry**](08_observability_and_telemetry.md) | **SREs & DevOps Engineers** | **Pipeline Telemetry & Monitoring.**<br>• Standalone DLT Event Log extraction and OpenTelemetry (OTel) payload builder.<br>• Databricks Volume and OTLP HTTP destination dispatchers.<br>• Structured in-pipeline JSON logging and comprehensive Error Handling Matrix. |
| **09** | [**🛠️ Developer Guide & Recipes**](09_developer_guide_and_recipes.md) | **Pipeline Developers** | **Step-by-Step Implementation & Cookbook.**<br>• 11-step walkthrough from empty JSON/YAML to verified deployment.<br>• Production-tested recipes for common integration patterns.<br>• Local testing with `pytest`, validation CLI, and troubleshooting runbooks. |
| **10** | [**💬 Multi-Role FAQs**](10_multi_role_faqs.md) | **Developers, Architects, PMs** | **Role-Specific Question & Answer Matrices.**<br>• **Developer FAQ**: practical syntax, debugging closures, local execution, edge cases.<br>• **Architect FAQ**: system decoupling, security boundaries, performance bottlenecks, cross-cloud egress.<br>• **Project Manager FAQ**: prerequisites, operational risks, timelines, governance guardrails. |
| **11** | [**🔐 Canonical Deterministic Hashing Standard**](11_hashing_and_determinism.md) | **Pipeline Developers & Data Stewards** | **The One Hash Construction (v1.3.0).**<br>• The single hash implementation shared by ingestion, CDC, and reconciliation (`__framework_hash_key`/`__framework_hash_value`).<br>• Reproducible Spark SQL for manual digest verification.<br>• v1.3.0 breaking-change migration checklist. |
| **12** | [**🧩 Master Module Permutation Matrix**](12_module_permutation_matrix.md) | **Architects & Pipeline Developers** | **Cross-Domain Compatibility Reference.**<br>• Which source types, CDC strategies, reconciliation scopes, and observability modes combine legally.<br>• A single consolidated list of explicitly unsupported combinations and why. |
| **13** | [**⚠️ Known Limitations, Silent Traps & Gotchas**](13_known_limitations_and_gotchas.md) | **Anyone authoring an onboarding JSON** | **What the validator cannot catch.**<br>• ~50 verified traps in one hyperlinked summary table, graded 🔴 Silent / 🟠 Late failure / 🟡 Inert / 🔵 Operational.<br>• Silent-data-loss cases (ZIP `target_volume_path` ≠ `path`, overwritten files, `schema_config` not projecting).<br>• Post-normalization column-naming rules every other spec field depends on.<br>• Lakeflow platform rules and deploy/ops traps that look like framework bugs. |
| **14** | [**📏 Onboarding Restrictions & Validation Rules**](14_onboarding_restrictions_and_validation_rules.md) | **Spec authors & agents** | **Every rule the onboarding gate enforces.**<br>• Mandatory fields per flow type, enumerations, forbidden and mode-incompatible configurations.<br>• How to test a spec without onboarding it. |
| **15** | [**🤖 Agent Skills & Prompt Library**](15_agent_skills_and_prompts.md) | **Platform engineers, agent users** | **Driving Metaflow with an LLM agent.**<br>• Task ↦ skill/tool ↦ prompt reference; the non-negotiable validate loop.<br>• Wiring the six tools into an agent framework. |
| **16** | [**🧭 Use Case Implementation Guide**](16_usecase_implementation_guide.md) | **Pipeline developers** | **The BT use cases end to end.**<br>• UC3 streaming CDC + batch recon, UC6 EA Flood Warning, UC7 CDR ASN.1 layouts and runbooks. |
| **17** | [**🔭 Framework Observability, AI/BI & Genie**](17_framework_observability_and_genie.md) | **SREs, analysts, architects** | **The store-and-query half of observability.**<br>• 11 `<catalog>.observability` views joining control metadata to system tables.<br>• The 10-page AI/BI dashboard, `AI_FORECAST` rules, the Genie space, the documentation job. |
| **Hub** | [**🏠 Landing page**](index.md) · [**Pillars**](pillars/index.md) · [**Console**](console/index.md) · [**Master configuration reference**](reference/json/index.md) · [**Docs ↔ code sync**](reference/sync.md) | **Everyone** | **The Material for MkDocs hub (v1.7.13).**<br>• Help-centre directory landing page, four pillar deep dives with tabs, badges and Mermaid.<br>• Console walkthroughs tab by tab; generated attribute reference with a schema tree, removed-attribute registry and four FAQs per attribute. |

---

## ⚡ Quick Navigation Decision Tree

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           WHAT IS YOUR OBJECTIVE?                               │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         │
        ┌────────────────────────────────┼────────────────────────────────┐
        ▼                                ▼                                ▼
I need to build a new            I need to lookup a              I need to evaluate
pipeline or debug an issue       JSON attribute or CDC logic     system architecture / FAQ
        │                                │                                │
        ▼                                ▼                                ▼
Open Developer Guide             Open Master Index               Open Platform Architecture
[09_developer_guide_and_recipes.md]  [00_master_reference_index.md]  [01_platform_architecture.md]
                                         │                                │
                                         ▼                                ▼
                                 Open Multi-Role FAQs            Open Governance / Security
                                 [10_multi_role_faqs.md]         [04_data_quality_and_governance.md]

  Is a specific field/combination legal? -> Open Module Permutation Matrix [12_module_permutation_matrix.md]
  Need the exact hash/digest construction? -> Open Hashing & Determinism [11_hashing_and_determinism.md]
```

---

## 🔗 Official Databricks Documentation References

All framework design patterns are built on verified Databricks platform capabilities:
- [Databricks Lakeflow Declarative Pipelines (DLT)](https://docs.databricks.com/aws/en/dlt/)
- [Auto Loader & Cloud Files Options](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/options.html)
- [Change Data Capture (`dlt.apply_changes`)](https://docs.databricks.com/en/delta-live-tables/cdc.html)
- [Unity Catalog Volumes](https://docs.databricks.com/aws/en/connect/unity-catalog/volumes)
- [Unity Catalog Secrets Management](https://docs.databricks.com/aws/en/security/secrets/unity-catalog-secrets)
- [Row Filters and Column Masks (ABAC)](https://docs.databricks.com/data-governance/unity-catalog/row-and-column-filters)
- [Delta Lake Change Data Feed (CDF)](https://docs.databricks.com/delta/delta-change-data-feed.html)
- [Databricks Asset Bundles (DABs)](https://docs.databricks.com/dev-tools/bundles/index.html)
