# Development Log 002: Config Layer & Attribute Registry

## Enactment Summary
- **Component**: 100% Data-Driven Configuration Layer & Registry
- **Specification Version**: v1.0 (Targeting Metaflow Framework Schema v1.3.0)
- **Status**: Completed

## Details of Enacted Functionality
1. **App Configuration (`config/index.json`)**:
   - Declared app settings, `obo` / `sp` auth modes, Databricks workspace host, storage roots (`vol_specs`, `ws_specs`), action profiles (`validate`, `onboard`), embedded MkDocs config, and feature flags.
2. **Metadata & Helper Encodings (`config/registry/_meta.json`)**:
   - Specified schema version 1.0, 10 widget kinds, label prefix stripping list, emission modes for 3-way helper attributes (`explode_mode`, `partition_mode`), and forbidden key denylist for secret hygiene.
3. **Attribute Registries (`config/registry/*.json`)**:
   - Created `root.json` (spec-level group ID, spark_config, pipeline_parameters, notes).
   - Created `observability.json` (observability_enabled, OTel & Volume destination repeat fields, framework columns).
   - Created `ingestion.json` (autoloader, zerobus, asn1, landing retention with degrade-to-off, ZIP handling with PGP pre-extraction, deduplication with event-time watermark, column normalization with case control, nested struct flattening and JSON string column parsing).
   - Created `transformation.json` (flow step identity, source inputs repeat with streaming watermarks, decrypted columns with cast-to-type, transformation SQL).
   - Created `reconciliation.json` (two-tier verification, Delta table-only dataset configuration, comparison direction, self-healing append target, matching keys, compare columns, transform SQL).
   - Created `shared.target.json` & `shared.cdc.json` (CDC strategy tabs, liquid clustering max 3, AES column encryption, sink config with PGP ZIP archiving, DQ rules & quarantine table, governance tags).
   - Created `fragments.json` (reusable `secret_ref` and `recon_dataset` fragments).
4. **Phases, Validation Rules, Docs, and Themes**:
   - Created ordered phase workflows in `config/phases/*.json`.
   - Created comprehensive cross-field validation rules in `config/validation/rules.json`.
   - Created deep-link documentation anchors in `config/docs.json`.
   - Extracted light and dark design tokens in `config/theme.json`.
