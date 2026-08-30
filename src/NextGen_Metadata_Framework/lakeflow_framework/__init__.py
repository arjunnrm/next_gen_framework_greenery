"""Shared, loosely-coupled modules for the metadata-driven Lakeflow framework.

Each subpackage owns exactly one concern (crypto, archive handling, ASN.1 decoding,
governance/ABAC, data quality, CDC strategies, storage properties, ingestion readers,
transformation inputs, control-plane metadata access, onboarding). Entrypoint notebooks
under 01_setup/, 02_onboarding/, and 03_engine/ import from here and stay thin
orchestration layers -- business logic lives in these modules so it can be unit tested,
reused, and enhanced independently of any single notebook.
"""
