"""GENERATED FILE - do not edit. Produced by scripts/apply_branding.py from branding/branding.json.

The Databricks App uploads only ``databricks-app/`` (the app resource sets
``source_code_path: "../../databricks-app"``), so the repo-root ``branding/``
package does not exist at runtime. This generated module embeds the same values
and has no imports outside the stdlib.
"""

from __future__ import annotations

from typing import Any, Dict

BRANDING: Dict[str, Any] = {
    '$comment': 'THE single source of truth for every user-visible brand string and logo asset in this repo. Edit this file, then run: python scripts/apply_branding.py. See branding/README.md for what is regenerated, what you must still edit by hand, and what is deliberately NOT configurable.',
    'app': {
        'deployed_name': 'metaflow-onboarding',
        'description': 'Self-service Metaflow Onboarding Databricks App for authoring, validating, diffing, and deploying metadata-driven ingestion, transformation, and reconciliation pipelines.',
        'running_message': 'Metaflow Onboarding App is running',
        'title': 'Metaflow Onboarding',
    },
    'consoles': {
        'control_dashboard_title': 'Metaflow Control Metadata',
        'dashboard_prefix': 'Metaflow',
        'genie_space_description': 'Ask about Metaflow dataflow groups: what each one does, which framework features it uses, how healthy its pipelines are, and what it costs.',
        'genie_space_title': 'Metaflow Framework Observability',
        'observability_dashboard_title': 'Metaflow Framework Observability',
    },
    'docs': {
        'copyright': 'Metaflow · NRM Analytix',
        'external_base_url': 'https://docs.internal.example.com/metaflow/',
        'site_description': 'Metadata-driven ingestion, transformation, reconciliation and observability on Databricks Lakeflow Declarative Pipelines.',
        'site_name': 'Metaflow',
    },
    'env': {
        'prefix': 'METAFLOW',
    },
    'framework': {
        'builder_subtitle': 'Spec Builder',
        'display_name': 'Metaflow',
        'slug': 'metaflow',
        'tagline': 'Metadata-driven ingestion, transformation and reconciliation on Databricks Lakeflow',
    },
    'logo': {
        'alpha_tolerance': 24,
        'alt_text': 'NRM Analytix',
        'background_to_alpha': True,
        'crop_to_content': True,
        'dark': {
            'background': [
                0,
                0,
                0,
            ],
            'note': 'Black background, white wordmark text. Correct for the DARK theme.',
            'published': 'logo-dark.png',
            'source': 'Logo Black.PNG',
        },
        'favicon': 'favicon.svg',
        'favicon_note': 'Unchanged. The existing databricks-app/web/public/favicon.svg carries no FlowX or hoonartek mark, so the rebrand as scoped does not require replacing it. An NRM-branded favicon is a NEW decision, not part of this rebrand.',
        'light': {
            'background': [
                255,
                255,
                255,
            ],
            'note': 'White background, dark (59,56,56) wordmark text. Correct for the LIGHT theme.',
            'published': 'logo-light.png',
            'source': 'Logo White.PNG',
        },
        'source_dir': 'logo',
    },
    'schema_version': '1.0',
    'vendor': {
        'display_name': 'NRM Analytix',
        'slug': 'nrm-analytix',
    },
}

ENV_PREFIX = "METAFLOW"

FRAMEWORK_NAME = BRANDING["framework"]["display_name"]
FRAMEWORK_SLUG = BRANDING["framework"]["slug"]
VENDOR_NAME = BRANDING["vendor"]["display_name"]
APP_TITLE = BRANDING["app"]["title"]
APP_DEPLOYED_NAME = BRANDING["app"]["deployed_name"]
APP_RUNNING_MESSAGE = BRANDING["app"]["running_message"]
DOCS_SITE_NAME = BRANDING["docs"]["site_name"]
DOCS_EXTERNAL_BASE_URL = BRANDING["docs"]["external_base_url"]


def get(dotted: str, default: Any = None) -> Any:
    """Look up a dotted path, e.g. ``get("app.title")``."""
    node: Any = BRANDING
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            return default
        node = node[part]
    return node


def env_var(name: str) -> str:
    """Build a prefixed environment variable name.

    ``env_var("SPEC_CATALOG") == "METAFLOW_SPEC_CATALOG"``. Already-prefixed
    names are returned unchanged, so this is safe to apply twice.
    """
    clean = str(name).strip().strip("_").upper()
    if not clean:
        raise ValueError("env_var() needs a non-empty name")
    if clean == ENV_PREFIX or clean.startswith(ENV_PREFIX + "_"):
        return clean
    return ENV_PREFIX + "_" + clean
