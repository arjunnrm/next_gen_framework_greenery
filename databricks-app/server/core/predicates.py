"""
Predicate DSL Evaluator for Metaflow Onboarding App.
Implements the §5.4 Specification:
Grammar:
  op := eq | ne | in | nin | gt | gte | lt | lte
      | set | unset | truthy | falsy | nonempty | empty | matches
      | and | or | not | always | never

Scoping:
  - Relative to current row/item
  - ^.path escapes one level up to parent scope
  - @path reaches spec root
  - Missing path resolves to None (falsy, empty, unset) without exceptions
  - Pure and memoized on (predicate_id, context_rev)
"""

import re
from functools import lru_cache
from typing import Any, Dict, List, Optional, Union


def resolve_path_value(path: str, context: Dict[str, Any]) -> Any:
    """Resolve a dotted path within hierarchical evaluation context.
    Scoping order:
      1. Explicit root escape '@' -> look in context['root']['v'] or context['v'] with '@'
      2. Explicit parent escape '^.' -> look in context['parent']
      3. Current scope context['v'] (or flat key in context)
      4. Fallback search in context['root']['v']
    """
    if not path or not isinstance(path, str):
        return None

    # Escape to root
    if path.startswith("@"):
        if path in context:
            return context[path]
        bare = path[1:]
        if bare in context:
            return context[bare]
        root = context.get("root", context)
        root_v = root.get("v", root) if isinstance(root, dict) else {}
        if isinstance(root_v, dict):
            if path in root_v:
                return root_v[path]
            if bare in root_v:
                return root_v[bare]
        return None

    # Escape to parent scope
    if path.startswith("^."):
        parent = context.get("parent", {})
        parent_v = parent.get("v", parent)
        sub_path = path[2:]
        return resolve_path_value(sub_path, {"v": parent_v, "root": context.get("root", context)})

    # Current scope
    v = context.get("v", context)
    if isinstance(v, dict):
        if path in v:
            return v[path]

    # kv and repeat widgets do not live in `v` -- they are stored under their own maps, keyed
    # by the same dotted path. Without this a rule referencing one (e.g.
    # sink_config.kafka_options) resolves to None whether it is filled in or not, so an
    # `empty` guard fires on a correctly-populated flow. Checked before the root fallback so a
    # flow-scoped kv always wins over a same-named root scalar.
    for bucket in ("kvs", "reps"):
        holder = context.get(bucket)
        if isinstance(holder, dict) and path in holder:
            return holder[path]

    # Check root as fallback if not in current scope
    if "root" in context:
        root_v = context["root"].get("v", context["root"])
        if isinstance(root_v, dict) and path in root_v:
            return root_v[path]

    return None


def is_empty_value(val: Any) -> bool:
    """Check if value is empty (None, empty/whitespace string, empty array, empty dict)."""
    if val is None:
        return True
    if isinstance(val, str):
        return len(val.strip()) == 0
    if isinstance(val, (list, tuple, dict, set)):
        return len(val) == 0
    return False


def is_truthy_value(val: Any) -> bool:
    """Check if value is truthy (True, 'true', non-zero number, non-empty collection)."""
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return val != 0
    if isinstance(val, str):
        low = val.strip().lower()
        if low in ("true", "1", "yes"):
            return True
        if low in ("false", "0", "no", ""):
            return False
        return len(val.strip()) > 0
    if isinstance(val, (list, tuple, dict, set)):
        return len(val) > 0
    return bool(val)


def evaluate_predicate(predicate: Optional[Dict[str, Any]], context: Dict[str, Any]) -> bool:
    """Evaluate a Predicate DSL AST against an evaluation context.
    Returns True or False. Never throws an exception on missing paths or type errors.
    """
    if predicate is None or not isinstance(predicate, dict) or not predicate:
        return True

    # Get the single operator key
    op = next(iter(predicate.keys()))
    args = predicate[op]

    # Special literal operators
    if op == "always":
        return True
    if op == "never":
        return False

    # Logical combinators
    if op == "and":
        if not isinstance(args, list):
            return True
        return all(evaluate_predicate(sub, context) for sub in args)

    if op == "or":
        if not isinstance(args, list):
            return False
        return any(evaluate_predicate(sub, context) for sub in args)

    if op == "not":
        if isinstance(args, dict):
            return not evaluate_predicate(args, context)
        return True

    # Helper for resolving target value and literal or reference
    def resolve_arg(arg: Any) -> Any:
        if isinstance(arg, dict) and "ref" in arg:
            return resolve_path_value(arg["ref"], context)
        return arg

    # Unary & Presence operators
    if op == "set":
        path = args[0] if isinstance(args, list) else args
        val = resolve_path_value(path, context)
        return val is not None and (not isinstance(val, str) or len(val) > 0)

    if op == "unset":
        path = args[0] if isinstance(args, list) else args
        val = resolve_path_value(path, context)
        return val is None or (isinstance(val, str) and len(val) == 0)

    if op == "truthy":
        path = args[0] if isinstance(args, list) else args
        val = resolve_path_value(path, context)
        return is_truthy_value(val)

    if op == "falsy":
        path = args[0] if isinstance(args, list) else args
        val = resolve_path_value(path, context)
        return not is_truthy_value(val)

    if op == "empty":
        path = args[0] if isinstance(args, list) else args
        val = resolve_path_value(path, context)
        return is_empty_value(val)

    if op == "nonempty":
        path = args[0] if isinstance(args, list) else args
        val = resolve_path_value(path, context)
        return not is_empty_value(val)

    # Binary comparison operators
    if isinstance(args, (list, tuple)) and len(args) >= 2:
        field_path = args[0]
        actual_raw = resolve_path_value(field_path, context)
        expected_raw = resolve_arg(args[1])

        if op == "eq":
            if actual_raw is None and expected_raw is None:
                return True
            # Handle boolean vs string comparison cleanly
            if isinstance(expected_raw, bool):
                return is_truthy_value(actual_raw) if expected_raw else not is_truthy_value(actual_raw)
            return str(actual_raw) == str(expected_raw) if (isinstance(actual_raw, (int, float, str)) and isinstance(expected_raw, (int, float, str))) else actual_raw == expected_raw

        if op == "ne":
            if isinstance(expected_raw, bool):
                return not (is_truthy_value(actual_raw) if expected_raw else not is_truthy_value(actual_raw))
            return actual_raw != expected_raw

        if op == "in":
            if not isinstance(expected_raw, (list, tuple, set)):
                expected_raw = [expected_raw]
            return actual_raw in expected_raw or str(actual_raw) in [str(x) for x in expected_raw]

        if op == "nin":
            if not isinstance(expected_raw, (list, tuple, set)):
                expected_raw = [expected_raw]
            return not (actual_raw in expected_raw or str(actual_raw) in [str(x) for x in expected_raw])

        if op in ("gt", "gte", "lt", "lte"):
            try:
                num_actual = float(actual_raw)
                num_expected = float(expected_raw)
                if op == "gt":
                    return num_actual > num_expected
                if op == "gte":
                    return num_actual >= num_expected
                if op == "lt":
                    return num_actual < num_expected
                if op == "lte":
                    return num_actual <= num_expected
            except (ValueError, TypeError):
                return False

        if op == "matches":
            if actual_raw is None:
                return False
            try:
                pattern = str(expected_raw)
                return bool(re.search(pattern, str(actual_raw)))
            except re.error:
                return False

    return False
