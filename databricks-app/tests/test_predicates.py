import json
from pathlib import Path
import pytest
from server.core.predicates import evaluate_predicate


def load_fixtures():
    fixture_file = Path(__file__).parent / "fixtures" / "predicates.json"
    with open(fixture_file, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.mark.parametrize("case", load_fixtures(), ids=lambda c: c["name"])
def test_predicate_fixtures(case):
    predicate = case["predicate"]
    context = case["context"]
    expected = case["expected"]
    actual = evaluate_predicate(predicate, context)
    assert actual == expected, f"Failed case: {case['name']} - Expected {expected}, got {actual}"
