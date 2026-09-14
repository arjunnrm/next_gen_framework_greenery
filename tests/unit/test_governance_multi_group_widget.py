"""The governance notebook's ``dataflow_group_id`` widget accepts a COMMA-SEPARATED LIST.

``notebooks/04_governance/04_apply_governance_and_egress.py`` used to take exactly one
``dataflow_group_id``, so tagging N groups meant N chained job tasks (see the previous shape of
``resources/uc3/uc3_governance_job.yml``). Since v0.0.7 it takes one id *or* a comma-separated
list, and ``uc3_governance_job.yml`` collapsed its two tasks into one.

A notebook is not importable (the ``# MAGIC``/``# COMMAND`` cells and the runtime-injected
``dbutils``/``spark`` globals make ``import`` impossible), so the two behaviours that actually
matter are pinned here differently:

* **Parsing** is re-implemented as a pure helper and asserted against the notebook's real source
  text, so the test fails if the notebook's parsing block is edited away from this contract.
* **Failure isolation** -- every group attempted, failures raised together at the end -- is
  asserted on a faithful transcription of the loop, because the alternative (bare ``raise`` on
  the first failure) would make ONE task carrying N groups strictly worse than N tasks: a bad
  group would silently deny its tags to every group listed after it.
"""

from pathlib import Path

import pytest

_NOTEBOOK = (
    Path(__file__).resolve().parents[2]
    / "notebooks"
    / "04_governance"
    / "04_apply_governance_and_egress.py"
)


def _parse_group_ids(raw):
    """The notebook's parsing contract: split, strip, drop blanks, de-duplicate, keep order."""
    group_ids = []
    for candidate in raw.split(","):
        candidate = candidate.strip()
        if candidate and candidate not in group_ids:
            group_ids.append(candidate)
    return group_ids


@pytest.mark.parametrize(
    "raw, expected",
    [
        # The single-id spelling is the one-element case -- every pre-v0.0.7 job keeps working.
        ("dfg_a", ["dfg_a"]),
        ("  dfg_a  ", ["dfg_a"]),
        # The new multi-group spelling, with and without whitespace around the separator.
        ("dfg_a,dfg_b", ["dfg_a", "dfg_b"]),
        ("dfg_a, dfg_b , dfg_c", ["dfg_a", "dfg_b", "dfg_c"]),
        # A trailing comma is an easy YAML hand-edit and must not fail the run.
        ("dfg_a,dfg_b,", ["dfg_a", "dfg_b"]),
        ("dfg_a,,dfg_b", ["dfg_a", "dfg_b"]),
        # De-duplication is order-preserving: a repeated id must not tag the group twice.
        ("dfg_b,dfg_a,dfg_b", ["dfg_b", "dfg_a"]),
        # Nothing usable -> empty, which the notebook turns into a ValueError.
        ("", []),
        ("   ", []),
        (",,", []),
    ],
)
def test_group_id_parsing(raw, expected):
    assert _parse_group_ids(raw) == expected


def test_uc3_group_string_parses_to_both_groups():
    """The exact string `resources/uc3/uc3_governance_job.yml` passes must yield both groups."""
    assert _parse_group_ids("dfg_uc3_excalibur_streaming_cdc,dfg_uc3_excalibur_batch_recon") == [
        "dfg_uc3_excalibur_streaming_cdc",
        "dfg_uc3_excalibur_batch_recon",
    ]


def test_notebook_still_implements_the_parsing_contract():
    """Pin the notebook's real source, so the helper above cannot drift from it silently."""
    source = _NOTEBOOK.read_text(encoding="utf-8")
    assert "GROUP_ID_RAW.split(\",\")" in source
    assert "if _candidate and _candidate not in GROUP_IDS:" in source
    assert "GROUP_IDS.append(_candidate)" in source


def test_notebook_widget_label_advertises_multi_group():
    source = _NOTEBOOK.read_text(encoding="utf-8")
    assert "comma-separated" in source


def test_notebook_accumulates_failures_instead_of_raising_on_the_first():
    """A bare `raise` inside the loop is the regression this guards against."""
    source = _NOTEBOOK.read_text(encoding="utf-8")
    assert "_failures.append" in source
    assert "for _group_id in GROUP_IDS:" in source
    # The end-of-run failure must name how many groups failed out of how many were attempted.
    assert "Governance pass failed for" in source


def _run_loop(group_ids, failing):
    """Faithful transcription of the notebook's loop, for behavioural assertions."""
    applied, failures = [], []
    for group_id in group_ids:
        try:
            if group_id in failing:
                raise RuntimeError(f"boom on {group_id}")
            applied.append(group_id)
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{group_id}: {exc}")
    return applied, failures


def test_every_group_is_attempted_even_when_an_earlier_one_fails():
    """The whole point of failure isolation: a bad FIRST group must not hide the rest."""
    applied, failures = _run_loop(["dfg_a", "dfg_b", "dfg_c"], failing={"dfg_a"})
    assert applied == ["dfg_b", "dfg_c"]
    assert len(failures) == 1
    assert "dfg_a" in failures[0]


def test_all_failures_are_reported_together():
    applied, failures = _run_loop(["dfg_a", "dfg_b", "dfg_c"], failing={"dfg_a", "dfg_c"})
    assert applied == ["dfg_b"]
    assert len(failures) == 2


def test_clean_run_records_no_failures():
    applied, failures = _run_loop(["dfg_a", "dfg_b"], failing=set())
    assert applied == ["dfg_a", "dfg_b"]
    assert failures == []
