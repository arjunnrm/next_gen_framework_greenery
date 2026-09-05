<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# JSON attribute reference

Every attribute the framework understands, grouped by the part of the spec it belongs to. Generated from the same registry the Spec Builder renders, so this reference and the app can never disagree.


## Sections

| Section | Attributes | What it covers |
|---|---|---|
| [Spec root](root.md) | 3 | Top-level attributes of the onboarding document. Everything else hangs off these. |
| [Ingestion flows](ingestion.md) | 110 | One entry per `ingestion_flows[]` element — reading from a landing zone into Bronze. |
| [Transformation flows](transformation.md) | 77 | One entry per `transformation_flows[]` element — SQL plus a CDC load strategy. |
| [CDC / load strategy](ingestion-transformation.md) | 10 | Attributes under `target_config` that only apply to particular CDC load strategies. The Spec Builder shows these on the **Load strategy** step and hides the ones the selected strategy does not use. |
| [Reconciliation flows](reconciliation.md) | 34 | One entry per `reconciliation_flows[]` element — comparing a baseline against targets. |
| [Observability](observability.md) | 18 | One entry per `observability[]` element — where telemetry is exported. |


**188 distinct attributes** across 6 sections.


## CDC load strategies

| Strategy | Applies to | What it does | Requires |
|---|---|---|---|
| `APPEND` | append-only feed | Immutable facts — events, logs, CDRs. No merge, no per-row diff. | no required parameters |
| `TRUNCATE_AND_LOAD` | full replace | Full extract each run, small enough to recompute entirely. | no required parameters |
| `SCD1` | overwrite current | Entity state where only the current value matters. | primary_keys required |
| `SCD2` | full history | Entity state where full history matters. | primary_keys required |
| `SCD3` | current + previous | Only current and previous value matter. Transformation flows only. | primary_keys + columns_to_check |
| `FULL_SNAPSHOT_CDC` | snapshot diff | Full extract each run, diffed against the previous one on a declared key to derive inserts, updates and deletes. | primary_keys required |
