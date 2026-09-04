# FlowX

Metadata-driven ingestion, transformation, reconciliation and observability pipelines on
Databricks Lakeflow. You describe *what* you want in one JSON or YAML document; the framework
compiles it into a Lakeflow pipeline and maintains the control tables behind it.

!!! tip "New here?"
    Start with **[Get started → Prerequisites](onboarding/01_prerequisites.md)**, then build
    something small with **[Your first pipeline](onboarding/02_first_pipeline.md)**.

---

## Pick your path

<div class="grid cards" markdown>

- :material-rocket-launch: **I want to onboard a pipeline**

    Work through the guided path, or open the Spec Builder app and let it write the JSON.

    [Get started](onboarding/index.md) · [Spec Builder app](onboarding/03_spec_builder_app.md)

- :material-book-search: **I need to look up an attribute**

    Every attribute, with type, default, allowed values and the rules that govern it.

    [Master attribute index](00_master_reference_index.md)

- :material-alert-decagram: **Something is behaving strangely**

    Roughly fifty verified traps the onboarding validator cannot catch, graded by how loudly
    they fail.

    [Known limitations & gotchas](13_known_limitations_and_gotchas.md) · [FAQ](faq.md)

- :material-sitemap: **I'm evaluating the architecture**

    The two-phase execution model, the control-table ERD, and how metadata stays decoupled
    from the Spark runtime.

    [Platform architecture](01_platform_architecture.md)

</div>

---

## What the framework does

| Stage | What you configure | What it produces |
|---|---|---|
| **Ingest** | A source type (Auto Loader, Zerobus, ASN.1), a landing path, and a target table | A Bronze streaming table, with technical-metadata and rescue columns |
| **Transform** | SQL plus a CDC load strategy | A Silver/Gold table maintained by `apply_changes`, with deterministic hash columns |
| **Assure** | Data-quality rules and governance tags | Pipeline expectations, quarantine routing, Unity Catalog tags |
| **Reconcile** | A baseline and one or more targets | Drift classification, per-record mismatch logs, optional self-healing |
| **Observe** | One or more telemetry destinations | OpenTelemetry payloads to a Volume or an OTLP collector |

Each of these is a *flow* inside a single spec document, and every flow in that document is
upserted into the control tables under one `dataflow_group_id`.

---

## The three ways in

1. **The Spec Builder app** — a Databricks App that renders the whole attribute registry as a
   guided form, validates as you go, and writes the spec to a Volume or Workspace path.
   See [Using the Spec Builder app](onboarding/03_spec_builder_app.md).
2. **Hand-authored JSON/YAML** — start from a template in `databricks-app/templates/` and edit
   directly. See the [Developer guide](09_developer_guide_and_recipes.md).
3. **Bulk config onboarding** — point the onboarding job at a directory of specs. See
   [Deploying with DABs](onboarding/04_deploying.md).

---

## Documentation map

| Module | Document | Audience |
|---|---|---|
| 00 | [Master reference index](00_master_reference_index.md) | Everyone |
| 01 | [Platform architecture](01_platform_architecture.md) | Architects |
| 02 | [Ingestion & sources](02_ingestion_and_sources.md) | Pipeline developers |
| 03 | [Transformation & CDC](03_transformation_and_cdc.md) | Pipeline developers |
| 04 | [Data quality & governance](04_data_quality_and_governance.md) | Governance leads |
| 05 | [Security & cryptography](05_security_and_cryptography.md) | Security engineers |
| 06 | [Egress & Lakeflow sinks](06_egress_and_lakeflow_sinks.md) | Integration engineers |
| 07 | [Reconciliation engine](07_reconciliation_engine.md) | Data stewards |
| 08 | [Observability & telemetry](08_observability_and_telemetry.md) | SREs |
| 09 | [Developer guide & recipes](09_developer_guide_and_recipes.md) | Pipeline developers |
| 10 | [Multi-role FAQs](10_multi_role_faqs.md) | Developers, architects, PMs |
| 11 | [Hashing & determinism](11_hashing_and_determinism.md) | Developers, stewards |
| 12 | [Module permutation matrix](12_module_permutation_matrix.md) | Architects |
| 13 | [Known limitations & gotchas](13_known_limitations_and_gotchas.md) | Anyone authoring a spec |

---

## Building this site

```bash
pip install mkdocs mkdocs-material
mkdocs serve     # http://127.0.0.1:8000 with live reload
mkdocs build     # static output in site/
```

`docs/archive/` and `docs/architecture_review/` are retained on disk for provenance but are
excluded from the built site — they are superseded content and a point-in-time audit
respectively, not living documentation.
