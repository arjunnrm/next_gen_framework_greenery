"""Native Lakeflow DQ expectations: warn / drop / fail, applied as a decorator.

``apply_dq_expectations`` wraps a staged view's ``@dlt.view`` function (see
``engine/flow_registration.py::register_staged_view``) with ``dlt.expect_all`` /
``dlt.expect_all_or_drop`` / ``dlt.expect_all_or_fail``, one call per action bucket, so a single
flow's ``dq_config.rules`` list can mix all three natively-supported actions in one DataFrame
without the caller having to know Lakeflow's expectation API shape at all -- it just passes the
raw rule list through.

``action: "quarantine"`` is the one rule action deliberately *not* handled here: Lakeflow has no
built-in "route failing rows to a different table" expectation, so quarantine rules are instead
evaluated manually as derived boolean/array columns by ``dq/quarantine.py::
add_quarantine_columns``, and the actual main/quarantine table split happens in that same
module's ``register_main_and_quarantine_tables``. This module and that one are companions,
covering the two halves of a flow's ``dq_config.rules`` between them.
"""

import logging
from typing import Any, Dict, List

import dlt

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError

logger = logging.getLogger("common.dq.expectations")

_SUPPORTED_NATIVE_ACTIONS = {"warn", "drop", "fail"}


def apply_dq_expectations(dq_rules: List[Dict[str, Any]]):
    """Decorator factory applying native ``warn`` / ``drop`` / ``fail`` DQ expectations.

    ``quarantine``-action rules are intentionally excluded here -- they are evaluated
    manually as derived columns (see :func:`common.dq.quarantine.add_quarantine_columns`)
    since Lakeflow has no built-in "route to a different table" expectation action.

    Raises
    ------
    FrameworkConfigError
        If a rule dict is missing ``rule_id``/``expression``/``action``, or specifies an
        unrecognized action (only ``warn``/``drop``/``fail``/``quarantine`` are valid).
    """
    try:
        for rule in dq_rules:
            missing = {"rule_id", "expression", "action"}.difference(rule)
            if missing:
                raise ValueError(f"DQ rule {rule} missing required key(s): {sorted(missing)}")
            if rule["action"] not in _SUPPORTED_NATIVE_ACTIONS | {"quarantine"}:
                raise ValueError(f"DQ rule '{rule['rule_id']}' has unsupported action '{rule['action']}'")

        warn_rules = {r["rule_id"]: r["expression"] for r in dq_rules if r["action"] == "warn"}
        drop_rules = {r["rule_id"]: r["expression"] for r in dq_rules if r["action"] == "drop"}
        fail_rules = {r["rule_id"]: r["expression"] for r in dq_rules if r["action"] == "fail"}
    except Exception as exc:  # noqa: BLE001
        raise FrameworkConfigError(f"Invalid dq_rules configuration: {exc}") from exc

    def decorator(func):
        wrapped = func
        if warn_rules:
            wrapped = dlt.expect_all(warn_rules)(wrapped)
        if drop_rules:
            wrapped = dlt.expect_all_or_drop(drop_rules)(wrapped)
        if fail_rules:
            wrapped = dlt.expect_all_or_fail(fail_rules)(wrapped)
        return wrapped

    return decorator
