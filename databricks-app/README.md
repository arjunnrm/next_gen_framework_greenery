# FlowX Onboarding Databricks App (v1.3.0)

A single-artifact Databricks App for authoring, validating, persisting, and onboarding metadata-driven pipelines to the **FlowX**.

---

## 1. Architectural Highlights

- **Unified Artifact**: Combines FastAPI backend (`server/`) and rich frontend single-page application (`web/dist/`) into one Databricks App deployment.
- **Strict Zero-Hardcoding**: No attribute name, enum value, dependency condition, validation rule, or template is hardcoded in code. Every single field lives in declarative JSON under `config/` and `templates/`.
- **Complete Schema Coverage (v1.3.0)**: Supports all 219+ documented attributes across Ingestion (Auto Loader, Zerobus, ASN.1 with PGP ZIP extraction), Transformation (12 CDC merge strategies, stream-stream joins with watermarks, column-level AES-GCM encryption/decryption), Reconciliation (two-tier verification, dataset pairing, append targets), and Observability (Databricks Volume & OTLP destinations).
- **Flexible Storage & Portability**:
  - **Load Spec**: Load from Unity Catalog Volume (`/Volumes/...`), Workspace directory (`/Workspace/...`), or upload directly from your laptop (JSON or YAML).
  - **Save Spec**: Export directly as a download to your laptop (JSON or YAML) or save directly to a Unity Catalog Volume or Workspace path with overwrite protection.
  - **Start from Template**: 18+ pre-built reference templates covering every pipeline architecture.
  - **Confirm & Onboard**: Validate in-app and deploy directly to Databricks execution jobs with live stage progress tracking and direct run links.
- **Embedded Framework Wiki**: `/docs` serves the complete documentation set (124 pages) — platform architecture, per-subsystem functional docs, onboarding walkthrough, the generated JSON attribute and code references, FAQs, known limitations, the architecture review and the archive — with tabbed navigation and full-text search. Every attribute's **docs** link deep-links to that attribute's own heading in the wiki, resolved through the generated `config/docs_index.json`.
- **Offline / Local Laptop Development**: Built-in mock mode (`FLOWX_FAKE_DBX=1`) allows complete local development and testing without requiring live Databricks credentials.

---

## 2. Directory Structure

```
flowx-onboarding-app/
├── app.yaml                        # Databricks Apps runtime configuration
├── requirements.txt                # Python backend dependencies
├── README.md                       # Comprehensive documentation & playbooks
├── config/                         # Declarative Config Layer (Source of Truth)
│   ├── index.json                  # App info, auth, actions, storage roots, limits
│   ├── docs.json                   # MkDocs anchor & deep-link routing
│   ├── theme.json                  # Design tokens & color palettes
│   ├── registry/                   # Schema attributes & form field definitions
│   │   ├── _meta.json              # Schema version, widget kinds, denylist
│   │   ├── fragments.json          # Reusable secret_ref & recon_dataset fragments
│   │   ├── root.json               # Root spec fields (dataflow_group_id, etc.)
│   │   ├── ingestion.json          # Ingestion flow attributes
│   │   ├── transformation.json     # Transformation flow attributes
│   │   ├── reconciliation.json     # Reconciliation flow attributes
│   │   ├── shared.cdc.json         # CDC merge strategies (APPEND, SCD1, SCD2, SCD3, etc.)
│   │   ├── shared.target.json      # Shared storage, partitioning, encryption, sinks
│   │   └── observability.json      # Observability destinations (Volume, OTLP)
│   ├── phases/                     # UI navigation workflows & phase definitions
│   │   ├── ingestion.json
│   │   ├── transformation.json
│   │   ├── reconciliation.json
│   │   └── spec.json
│   └── validation/
│       └── rules.json              # Cross-field business validation rules
├── templates/                      # Catalogue of reference templates
│   ├── index.json                  # Template index & category definitions
│   ├── pipeline_onboarding_template.json # Canonical v1.3.0 master reference spec
│   ├── ingestion/                  # Ingestion templates (autoloader, zerobus, asn1, json)
│   ├── transformation/             # Transformation templates (append, scd1, scd2, scd3, sinks)
│   └── reconciliation/             # Reconciliation templates (full, warn)
├── server/                         # FastAPI Application Backend
│   ├── app.py                      # FastAPI entrypoint, middleware, static mounts
│   ├── settings.py                 # Pydantic settings & validation
│   ├── deps.py                     # Per-request dependency injection
│   ├── errors.py                   # Standard error envelope
│   ├── logging_setup.py            # Structured JSON logger
│   ├── core/                       # Processing Engines
│   │   ├── predicates.py           # Memoized Predicate DSL evaluator
│   │   ├── registry.py             # Registry loader, fragment resolver & integrity verifier
│   │   ├── serializer.py           # 8-step deterministic serializer (JSON/YAML)
│   │   ├── deserializer.py         # Lossless spec parser with helper reverse derivations
│   │   ├── validator.py            # Layer 1 + Layer 2 validator & secret hygiene
│   │   └── diff.py                 # Structural JSON-pointer diff engine
│   ├── clients/                    # Databricks Integration
│   │   ├── dbx.py                  # Client provider (obo, sp, and offline mock client)
│   │   ├── files.py                # Volume & Workspace file manager with traversal checks
│   │   ├── jobs.py                 # Action runner & stage progression poller
│   │   └── access.py               # Preflight permission checklist
│   └── routers/                    # REST Endpoints
│       ├── config_router.py        # /api/health, /api/config, /api/templates
│       ├── spec_router.py          # /api/spec/validate, /api/spec/render, /api/spec/import, /api/spec/diff
│       ├── workspace_router.py     # /api/workspace/access, /api/workspace/list, /api/workspace/read, /api/workspace/write
│       ├── onboard_router.py       # /api/actions/{action_id}/run, /api/actions/runs/{run_id}
│       ├── docs_router.py          # /api/docs/resolve
│       └── debug_router.py         # /api/debug/config, /api/debug/predicate
├── web/                            # Frontend Single-Page Application
│   ├── src/                        # TypeScript source code
│   │   ├── engine/                 # Client-side predicates and resolve engines
│   │   ├── api/                    # REST API client
│   │   ├── state/                  # Reactive in-memory state store
│   │   ├── components/             # UI widgets, headers, sidebars, canvases, drawers, modals
│   │   └── styles/                 # CSS Design System
│   └── dist/                       # Pre-built zero-dependency static distribution
│       ├── index.html
│       ├── styles.css
│       └── app.js
├── docs_site/                      # Full MkDocs wiki (GENERATED — do not edit)
│   └── ...                         # 124 pages; built by scripts/build_app_docs.py
├── tests/                          # Automated Pytest Test Suite
│   ├── fixtures/predicates.json    # Predicate DSL test corpus
│   ├── test_predicates.py          # DSL evaluator tests
│   ├── test_registry_integrity.py  # Startup registry integrity assertions
│   ├── test_serializer_roundtrip.py# Full spec roundtrip tests
│   ├── test_validation_rules.py    # Cross-field rule tests
│   ├── test_api_contract.py        # FastAPI endpoint tests
│   ├── test_access_check.py        # Access checklist & action runner tests
│   ├── test_paths.py               # Security & path traversal tests
│   └── test_frontend_serving.py    # Static asset and SPA route tests
└── dev_logs/                       # Detailed Milestone Enactment Logs
```

---

## 3. Local Development & Testing

### Running the Test Suite
```bash
$env:PYTHONPATH="flowx-onboarding-app"
.\.venv\Scripts\python.exe -m pytest flowx-onboarding-app/tests/ -v
```

### Rebuilding the Embedded Wiki
`docs_site/` and `config/docs_index.json` are generated. After editing anything under the repo's
`docs/` directory, regenerate and re-verify the deep links:
```bash
python scripts/build_app_docs.py           # rebuild + sync + reindex
python scripts/build_app_docs.py --check   # CI: fail if the committed output is stale
```

### Running the App Locally (Offline / Laptop Mode)
```bash
$env:PYTHONPATH="flowx-onboarding-app"
$env:FLOWX_FAKE_DBX="1"
.\.venv\Scripts\python.exe -m uvicorn server.app:app --host 0.0.0.0 --port 8000 --reload
```
Open your browser at `http://localhost:8000/`.

---

## 4. Databricks DABs Bundle Deployment

The app is integrated into the workspace bundle via `resources/flowx_app/flowx_onboarding_app.yml`.

### Deploying to Dev Target
```bash
databricks bundle deploy -t dev
```

### Starting the App in Databricks
```bash
databricks apps start flowx_onboarding_app
```

---

## 5. Extension Playbooks (§14)

### Playbook 1: Adding a New Attribute
1. Open the appropriate registry file (`config/registry/ingestion.json`, `transformation.json`, or `reconciliation.json`).
2. Add the field descriptor to the relevant section's `fields` array:
```json
{
  "path": "target_config.my_new_flag",
  "label": "my_new_flag",
  "widget": "boolean",
  "type": "boolean",
  "default": false,
  "description": "Enable custom pipeline optimization."
}
```
3. If it requires documentation deep-linking, add the anchor in `config/docs.json`.
4. Run `pytest` — the app and registry integrity tests will automatically validate the new field!

### Playbook 2: Adding a New CDC Strategy
1. Open `config/registry/shared.cdc.json`.
2. Add the strategy to the `tabs` array:
```json
{ "id": "my_strategy", "value": "MY_STRATEGY", "label": "Custom Strategy" }
```
3. Define its specific parameter fields with `"visible_when": { "eq": ["target_config.cdc_load_strategy", "MY_STRATEGY"] }`.

### Playbook 3: Adding a Cross-Field Validation Rule
1. Open `config/validation/rules.json`.
2. Add the rule to the `rules` array:
```json
{
  "id": "my_rule_id",
  "severity": "error",
  "scope": "flow",
  "when": {
    "and": [
      { "eq": ["target_config.cdc_load_strategy", "MY_STRATEGY"] },
      { "empty": ["target_config.my_required_field"] }
    ]
  },
  "message": "my_required_field is required when strategy is MY_STRATEGY.",
  "field_path": "target_config.my_required_field"
}
```
3. Add a unit test case in `tests/test_validation_rules.py`.

### Playbook 4: Adding a Reference Template
1. Create a spec JSON file under `templates/ingestion/`, `transformation/`, or `reconciliation/`.
2. Register the template in `templates/index.json` under the `templates` array.
