"""
Lossless Deserializer for FlowX Onboarding App.
Converts incoming framework JSON/YAML spec into Flow Document Store (SpecDoc) according to §8.5.
"""

import copy
import json
from typing import Any, Dict, List, Optional, Tuple, Union
import uuid
import yaml

from server.core.registry import RegistryManager


class SpecDeserializer:
    """Deserializes framework JSON or YAML into an in-memory SpecDoc."""

    def __init__(self, registry_manager: Optional[RegistryManager] = None):
        if registry_manager is None:
            registry_manager = RegistryManager()
        self.reg = registry_manager

    def _flatten_dict(self, d: Dict[str, Any], prefix: str = "") -> List[Tuple[str, Any]]:
        """Flatten a nested dict into a list of (dotted_path, value) pairs."""
        items: List[Tuple[str, Any]] = []
        for k, v in d.items():
            full_key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict) and not self._is_kv_field(full_key):
                items.extend(self._flatten_dict(v, full_key))
            else:
                items.append((full_key, v))
        return items

    def _is_kv_field(self, path: str) -> bool:
        """Check if a dotted path is configured as a kv widget."""
        for fmap in self.reg.field_maps.values():
            if path in fmap and fmap[path].get("widget") == "kv":
                return True
        return False

    def _deserialize_repeat_item(self, row: Dict[str, Any], child_field_defs: List[Dict[str, Any]], flow_kind: str) -> Dict[str, Any]:
        """Deserialize a single repeat row item into RowDoc format."""
        row_doc: Dict[str, Any] = {"__id": f"row_{uuid.uuid4().hex[:8]}"}
        row_kvs: Dict[str, List[List[str]]] = {}

        # Decompose three-part table if present
        if "table" in row and isinstance(row["table"], str) and flow_kind == "reconciliation":
            parts = row["table"].split(".")
            if len(parts) == 3:
                row_doc["target_catalog"] = parts[0]
                row_doc["target_schema"] = parts[1]
                row_doc["target_table"] = parts[2]

        flat_pairs = self._flatten_dict(row)
        child_map = {f["path"]: f for f in child_field_defs}

        for path, val in flat_pairs:
            f = child_map.get(path)
            if not f:
                # Store verbatim for preservation
                if isinstance(val, (str, int, float, bool)):
                    row_doc[path] = val
                continue

            widget = f.get("widget", "text")
            if widget == "kv" and isinstance(val, dict):
                row_kvs[path] = [[str(k), str(v)] for k, v in val.items()]
            elif widget == "list":
                if isinstance(val, list):
                    row_doc[path] = ", ".join(str(x) for x in val)
                elif val is not None:
                    row_doc[path] = str(val)
            else:
                row_doc[path] = val

        if row_kvs:
            row_doc["__kv"] = row_kvs

        return row_doc

    def deserialize_flow(self, flow_dict: Dict[str, Any], flow_kind: str) -> Tuple[Dict[str, Any], List[str]]:
        """Deserialize a single flow dictionary into FlowDoc format."""
        flow_doc: Dict[str, Any] = {
            "id": f"flow_{uuid.uuid4().hex[:8]}",
            "v": {},
            "kvs": {},
            "reps": {}
        }
        unknown_paths: List[str] = []
        fmap = self.reg.field_maps.get(flow_kind, {})

        # Derive helper fields in reverse (§8.5)
        # 1. explode_mode derivation
        if "source_config" in flow_dict and isinstance(flow_dict["source_config"], dict):
            src_cfg = flow_dict["source_config"]
            if "explode_columns" not in src_cfg:
                flow_doc["v"]["source_config.explode_mode"] = "absent"
            elif src_cfg["explode_columns"] == []:
                flow_doc["v"]["source_config.explode_mode"] = "empty"
            else:
                flow_doc["v"]["source_config.explode_mode"] = "named"

        # 2. partition_mode derivation
        if "target_config" in flow_dict and isinstance(flow_dict["target_config"], dict):
            tgt_cfg = flow_dict["target_config"]
            if "partition_columns" not in tgt_cfg:
                flow_doc["v"]["target_config.partition_mode"] = "absent"
            elif tgt_cfg["partition_columns"] == []:
                flow_doc["v"]["target_config.partition_mode"] = "unpartitioned"
            else:
                flow_doc["v"]["target_config.partition_mode"] = "named"

        # 3. Handle un-grouping decrypted_columns from source_inputs
        if flow_kind == "transformation" and "source_inputs" in flow_dict:
            flat_decrypted = []
            for inp in flow_dict.get("source_inputs", []):
                input_name = inp.get("input_name", "")
                for dec in inp.get("decrypted_columns", []):
                    dec_copy = copy.deepcopy(dec)
                    dec_copy["input_name"] = input_name
                    flat_decrypted.append(dec_copy)

            if flat_decrypted:
                dec_def = fmap.get("decrypted_columns", {})
                child_fields = dec_def.get("fields", [])
                flow_doc["reps"]["decrypted_columns"] = [
                    self._deserialize_repeat_item(row, child_fields, flow_kind)
                    for row in flat_decrypted
                ]

        # 4. Flatten and assign fields
        flat_pairs = self._flatten_dict(flow_dict)
        for path, val in flat_pairs:
            f = fmap.get(path)

            if f is None:
                # Check if it's a known top-level repeat array
                if path in fmap and fmap[path].get("widget") == "repeat":
                    f = fmap[path]
                else:
                    unknown_paths.append(path)
                    continue

            widget = f.get("widget", "text")

            if widget == "kv":
                if isinstance(val, dict):
                    flow_doc["kvs"][path] = [[str(k), str(v)] for k, v in val.items()]
                elif isinstance(val, list):
                    flow_doc["kvs"][path] = val
            elif widget == "repeat":
                if isinstance(val, list):
                    child_fields = f.get("fields", [])
                    # Special case: do not double-process source_inputs decrypted_columns
                    flow_doc["reps"][path] = [
                        self._deserialize_repeat_item(row, child_fields, flow_kind)
                        for row in val
                    ]
            elif widget == "list":
                if isinstance(val, list):
                    # Handle json_string_columns which might contain objects
                    if path == "source_config.json_string_columns":
                        items = []
                        for x in val:
                            if isinstance(x, dict) and "column" in x:
                                items.append(f"{x['column']}:{x.get('schema_ddl','')}" if x.get("schema_ddl") else x["column"])
                            else:
                                items.append(str(x))
                        flow_doc["v"][path] = ", ".join(items)
                    else:
                        flow_doc["v"][path] = ", ".join(str(x) for x in val)
                elif val is not None:
                    flow_doc["v"][path] = str(val)
            else:
                flow_doc["v"][path] = val

        return flow_doc, unknown_paths

    def deserialize_spec(self, content_or_dict: Union[str, Dict[str, Any]], fmt: str = "json") -> Dict[str, Any]:
        """Parse raw JSON/YAML string or dict into complete SpecDoc."""
        if isinstance(content_or_dict, str):
            if fmt.lower() in ("yaml", "yml"):
                raw_spec = yaml.safe_load(content_or_dict) or {}
            else:
                raw_spec = json.loads(content_or_dict)
        else:
            raw_spec = content_or_dict

        spec_doc: Dict[str, Any] = {
            "rev": 1,
            "templateVars": {"catalog": "flowx", "env": "dev"},
            "root": {
                "id": "root_doc",
                "v": {},
                "kvs": {},
                "reps": {}
            },
            "observability": {
                "enabled": False,
                "destinations": []
            },
            "ingestion_flows": [],
            "transformation_flows": [],
            "reconciliation_flows": [],
            "meta": {
                "unknown": [],
                "warnings": []
            }
        }

        # 1. Spec Root
        group_id = raw_spec.get("dataflow_group_id")
        if group_id:
            spec_doc["root"]["v"]["@dataflow_group_id"] = group_id
            spec_doc["root"]["v"]["dataflow_group_id"] = group_id

        if "pipeline_parameters" in raw_spec and isinstance(raw_spec["pipeline_parameters"], dict):
            spec_doc["root"]["kvs"]["@pipeline_parameters"] = [
                [str(k), str(v)] for k, v in raw_spec["pipeline_parameters"].items()
            ]

        if "spark_config" in raw_spec and isinstance(raw_spec["spark_config"], dict):
            spec_doc["root"]["kvs"]["@spark_config"] = [
                [str(k), str(v)] for k, v in raw_spec["spark_config"].items()
            ]

        # 2. Ingestion flows
        for flow_dict in raw_spec.get("ingestion_flows", []):
            flow_doc, unk = self.deserialize_flow(flow_dict, "ingestion")
            spec_doc["ingestion_flows"].append(flow_doc)
            spec_doc["meta"]["unknown"].extend([f"ingestion_flows.{p}" for p in unk])

        # 3. Transformation flows
        for flow_dict in raw_spec.get("transformation_flows", []):
            flow_doc, unk = self.deserialize_flow(flow_dict, "transformation")
            spec_doc["transformation_flows"].append(flow_doc)
            spec_doc["meta"]["unknown"].extend([f"transformation_flows.{p}" for p in unk])

        # 4. Reconciliation flows
        for flow_dict in raw_spec.get("reconciliation_flows", []):
            flow_doc, unk = self.deserialize_flow(flow_dict, "reconciliation")
            spec_doc["reconciliation_flows"].append(flow_doc)
            spec_doc["meta"]["unknown"].extend([f"reconciliation_flows.{p}" for p in unk])

        # 5. Observability
        if "observability" in raw_spec and isinstance(raw_spec["observability"], list):
            spec_doc["root"]["v"]["@observability_enabled"] = True
            obs_field_def = self.reg.field_maps.get("observability", {}).get("@observability", {})
            obs_child_fields = obs_field_def.get("fields", [])

            obs_rows = [
                self._deserialize_repeat_item(row, obs_child_fields, "observability")
                for row in raw_spec["observability"]
            ]
            spec_doc["root"]["reps"]["@observability"] = obs_rows
            spec_doc["observability"]["enabled"] = True

        return spec_doc


def as_spec_doc(
    spec: Optional[Dict[str, Any]],
    registry_manager: Optional[RegistryManager] = None,
) -> Dict[str, Any]:
    """Accept either an internal SpecDoc or a canonical framework spec; return a SpecDoc.

    Two shapes reach the API:

    * the app's internal SpecDoc — ``{"root": {"v": ..., "kvs": ..., "reps": ...}, ...}``
    * the canonical framework spec the user sees in the preview pane and saves to a
      Volume — ``{"dataflow_group_id": ..., "ingestion_flows": [...]}``

    The React frontend posts the canonical shape (``Builder.spec()``) to
    ``/api/actions/{id}/run``, while the validator and serializer both operate on
    SpecDoc. Normalising here lets one endpoint serve both callers instead of
    forcing the frontend to expose its internal representation.

    A SpecDoc is passed through untouched, so existing callers are unaffected.
    """
    if not isinstance(spec, dict):
        return {}
    if "root" in spec:
        return spec
    inner = spec.get("spec")
    if isinstance(inner, dict):
        if "root" in inner:
            return inner
        return SpecDeserializer(registry_manager).deserialize_spec(inner)
    return SpecDeserializer(registry_manager).deserialize_spec(spec)
