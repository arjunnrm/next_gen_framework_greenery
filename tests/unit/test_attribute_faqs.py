"""Every onboarding-spec attribute carries at least four grounded FAQs (v1.7.13).

``databricks-app/config/attribute_faqs.json`` is rendered under each attribute in
``docs/reference/json/*`` by ``scripts/build_docs_reference.py``. The registry
(``attribute_knowledge.json``) is the source of truth for *which* attributes exist, so these
tests make the two files move together: add an attribute to ``registry.js`` and the docs
build is red until its FAQs are written; delete one and its orphaned FAQs are flagged.

No Spark, no imports from the wheel: JSON files only, so this runs in the docs CI job.
"""

import json
from pathlib import Path

import pytest

KNOWLEDGE = Path("databricks-app/config/attribute_knowledge.json")
FAQS = Path("databricks-app/config/attribute_faqs.json")

REQUIRED_KINDS = {"omitted", "format", "performance", "edge_case"}
MIN_FAQS_PER_ATTRIBUTE = 4
MAX_ANSWER_WORDS = 120


def _knowledge_paths() -> set:
    return set(json.loads(KNOWLEDGE.read_text(encoding="utf-8"))["attributes"])


def _faqs() -> dict:
    data = json.loads(FAQS.read_text(encoding="utf-8"))
    assert isinstance(data, dict) and "attributes" in data, "attribute_faqs.json must have an 'attributes' object"
    return data["attributes"]


def test_faq_file_declares_the_four_kinds():
    data = json.loads(FAQS.read_text(encoding="utf-8"))
    assert set(data.get("kinds", {})) == REQUIRED_KINDS


def test_no_faq_names_an_attribute_the_registry_does_not_have():
    orphans = sorted(set(_faqs()) - _knowledge_paths())
    assert not orphans, f"FAQs for attributes that are not in the registry: {orphans}"


@pytest.mark.parametrize("path", sorted(_knowledge_paths()))
def test_every_registry_attribute_has_at_least_four_faqs_covering_every_kind(path):
    entries = _faqs().get(path, [])
    assert len(entries) >= MIN_FAQS_PER_ATTRIBUTE, (
        f"{path}: {len(entries)} FAQ(s); at least {MIN_FAQS_PER_ATTRIBUTE} are required "
        "(write them in databricks-app/config/attribute_faqs.json)"
    )
    kinds = {e.get("kind") for e in entries}
    missing = REQUIRED_KINDS - kinds
    assert not missing, f"{path}: FAQs missing the kind(s) {sorted(missing)}"


@pytest.mark.parametrize("path", sorted(_knowledge_paths()))
def test_every_faq_is_well_formed(path):
    for i, entry in enumerate(_faqs().get(path, [])):
        assert entry.get("kind") in REQUIRED_KINDS, f"{path}[{i}]: unknown kind {entry.get('kind')!r}"
        q, a = (entry.get("q") or "").strip(), (entry.get("a") or "").strip()
        assert q.endswith("?"), f"{path}[{i}]: question must end with '?': {q!r}"
        assert len(a) >= 20, f"{path}[{i}]: answer is too short to be useful"
        assert len(a.split()) <= MAX_ANSWER_WORDS, f"{path}[{i}]: answer exceeds {MAX_ANSWER_WORDS} words"
        assert "\n#" not in a and not a.startswith("#"), f"{path}[{i}]: no markdown headings inside an answer"


def test_questions_are_not_copy_pasted_across_attributes():
    """A question repeated verbatim on many attributes is a template, not a FAQ."""
    seen: dict = {}
    for path, entries in _faqs().items():
        for e in entries:
            seen.setdefault((e.get("q") or "").strip().lower(), set()).add(path)
    # Allow a little reuse for the nine secret_* sub-fields; flag anything beyond that.
    offenders = {q: sorted(p) for q, p in seen.items() if len(p) > 6}
    assert not offenders, f"questions reused on more than six attributes: {offenders}"
