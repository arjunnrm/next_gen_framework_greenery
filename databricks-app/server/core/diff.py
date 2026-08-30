"""
Deep Structural Spec Diff Engine for Metaflow Onboarding App.
Compares a newly authored/modified spec against an existing spec to compute
added, removed, and changed attributes/flows for the confirm step before onboarding.
"""

from typing import Any, Dict, List, Tuple


class SpecDiffer:
    """Computes JSON-pointer level structural diffs between two specs."""

    @staticmethod
    def diff_specs(new_spec: Dict[str, Any], old_spec: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Compare new_spec against old_spec.
        Returns:
          {
            "added": [ { "path": "/pointer", "value": Any } ],
            "removed": [ { "path": "/pointer", "old_value": Any } ],
            "changed": [ { "path": "/pointer", "old_value": Any, "new_value": Any } ]
          }
        """
        added: List[Dict[str, Any]] = []
        removed: List[Dict[str, Any]] = []
        changed: List[Dict[str, Any]] = []

        def compare_nodes(new_node: Any, old_node: Any, current_path: str):
            if isinstance(new_node, dict) and isinstance(old_node, dict):
                all_keys = set(new_node.keys()).union(set(old_node.keys()))
                for k in all_keys:
                    p = f"{current_path}/{k}"
                    if k in new_node and k not in old_node:
                        added.append({"path": p, "value": new_node[k]})
                    elif k in old_node and k not in new_node:
                        removed.append({"path": p, "old_value": old_node[k]})
                    else:
                        compare_nodes(new_node[k], old_node[k], p)
            elif isinstance(new_node, list) and isinstance(old_node, list):
                max_len = max(len(new_node), len(old_node))
                for i in range(max_len):
                    p = f"{current_path}/{i}"
                    if i < len(new_node) and i >= len(old_node):
                        added.append({"path": p, "value": new_node[i]})
                    elif i < len(old_node) and i >= len(new_node):
                        removed.append({"path": p, "old_value": old_node[i]})
                    else:
                        compare_nodes(new_node[i], old_node[i], p)
            else:
                if new_node != old_node:
                    changed.append({"path": current_path, "old_value": old_node, "new_value": new_node})

        compare_nodes(new_spec, old_spec, "")
        return {
            "added": added,
            "removed": removed,
            "changed": changed
        }
