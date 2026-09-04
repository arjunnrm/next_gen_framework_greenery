"""
Deterministic Serializer for FlowX Onboarding App.
Converts Flow Document Store (SpecDoc) to framework JSON/YAML according to §8.
"""

import copy
import json
from typing import Any, Dict, List, Optional, Tuple, Union
import yaml

from server.core.predicates import evaluate_predicate
from server.core.registry import RegistryManager


class SpecSerializer:
    """Serializes in-memory SpecDoc to schema-compliant framework JSON or YAML."""

    def __init__(self, registry_manager: Optional[RegistryManager] = None):
        if registry_manager is None:
            registry_manager = RegistryManager()
        self.reg = registry_manager

    def resolve_field_value(self, field_def: Dict[str, Any], context: Dict[str, Any]) -> Any:
        """Resolve value for a field descriptor following §6.3:
        1. raw value from context if present and not empty string
        2. first matching default_when clause
        3. field default
        """
        path = field_def.get("path", "")
        v_dict = context.get("v", {})

        # Raw value
        if path in v_dict and v_dict[path] is not None and v_dict[path] != "":
            return v_dict[path]

        # Conditional defaults
        for entry in field_def.get("default_when", []):
            when_pred = entry.get("when")
            if evaluate_predicate(when_pred, context):
                return entry.get("value")

        # Static default
        return field_def.get("default")

    def _set_dotted_path(self, target_dict: Dict[str, Any], dotted_path: str, value: Any) -> None:
        """Set a value in a nested dictionary by dotted path."""
        parts = dotted_path.split(".")
        cur = target_dict
        for i, part in enumerate(parts[:-1]):
            if part not in cur or not isinstance(cur[part], dict):
                cur[part] = {}
            cur = cur[part]
        cur[parts[-1]] = value

    def _serialize_repeat_item(self, item: Dict[str, Any], fields: List[Dict[str, Any]], context: Dict[str, Any], flow_kind: str) -> Dict[str, Any]:
        """Serialize a single repeat row item."""
        row_out: Dict[str, Any] = {}
        row_context = {
            "v": item,
            "parent": context,
            "root": context.get("root", context),
            "@__flow_kind": flow_kind
        }

        # Handle special composition: reconciliation target table
        if "target_catalog" in item and "target_schema" in item and "target_table" in item:
            cat = str(item.get("target_catalog", "")).strip()
            sch = str(item.get("target_schema", "")).strip()
            tbl = str(item.get("target_table", "")).strip()
            if cat and sch and tbl:
                row_out["table"] = f"{cat}.{sch}.{tbl}"
                row_out["type"] = "table"

        for f in fields:
            path = f.get("path", "")
            if f.get("emit") == "never" and path in ("target_catalog", "target_schema", "target_table"):
                continue

            # Check visibility
            vis_pred = f.get("visible_when")
            if vis_pred and not evaluate_predicate(vis_pred, row_context):
                continue

            val = self.resolve_field_value(f, row_context)
            if val is None or val == "":
                continue

            # Coerce value based on widget and type
            val = self._coerce_field_value(val, f)
            if val is not None and val != "" and val != [] and val != {}:
                emit_path = f.get("emit_path", path)
                self._set_dotted_path(row_out, emit_path, val)

        # Handle nested __kv in repeat row
        if "__kv" in item and isinstance(item["__kv"], dict):
            for k, kv_list in item["__kv"].items():
                kv_obj = {row[0]: row[1] for row in kv_list if len(row) >= 2 and row[0]}
                if kv_obj:
                    self._set_dotted_path(row_out, k, kv_obj)

        return row_out

    def _coerce_field_value(self, val: Any, field_def: Dict[str, Any]) -> Any:
        """Coerce raw value according to widget and type descriptor."""
        widget = field_def.get("widget", "text")
        target_type = field_def.get("type", "string")

        if widget == "list":
            if isinstance(val, str):
                items = [x.strip() for x in val.split(",") if x.strip()]
                return items
            elif isinstance(val, list):
                return [x.strip() if isinstance(x, str) else x for x in val if x]
            return []

        if widget == "number" or target_type in ("number", "integer"):
            try:
                return int(val) if target_type == "integer" or (isinstance(val, (int, float, str)) and str(val).isdigit()) else float(val)
            except (ValueError, TypeError):
                return val

        if widget == "boolean" or target_type == "boolean":
            if isinstance(val, bool):
                return val
            if isinstance(val, str):
                return val.strip().lower() in ("true", "1", "yes")
            return bool(val)

        return val

    def serialize_flow(self, flow_doc: Dict[str, Any], flow_kind: str, root_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize an individual flow document (ingestion, transformation, or reconciliation)."""
        flow_out: Dict[str, Any] = {}
        v_dict = copy.deepcopy(flow_doc.get("v", {}))
        kvs_dict = flow_doc.get("kvs", {})
        reps_dict = flow_doc.get("reps", {})

        # Field definitions for this flow kind
        fmap = self.reg.field_maps.get(flow_kind, {})

        # Pre-fill resolved default values so predicates see effective values (§6.3)
        effective_v = {}
        for path, f in fmap.items():
            if path.startswith("@"):
                continue
            if path in v_dict and v_dict[path] is not None and v_dict[path] != "":
                effective_v[path] = v_dict[path]
            elif f.get("default") is not None:
                effective_v[path] = f.get("default")

        # Merge root defaults
        root_v = root_doc.get("v", {})
        for path, f in self.reg.field_maps.get("root", {}).items():
            if path in root_v and root_v[path] is not None and root_v[path] != "":
                effective_v[path] = root_v[path]
            elif f.get("default") is not None:
                effective_v[path] = f.get("default")

        context = {
            "v": effective_v,
            "root": {"v": effective_v},
            "@__flow_kind": flow_kind
        }

        # Handle UI-only helper fields (emission modes)
        explode_mode = v_dict.get("source_config.explode_mode")
        if explode_mode == "empty":
            self._set_dotted_path(flow_out, "source_config.explode_columns", [])
        elif explode_mode == "absent":
            pass

        partition_mode = v_dict.get("target_config.partition_mode")
        if partition_mode == "unpartitioned":
            self._set_dotted_path(flow_out, "target_config.partition_columns", [])
        elif partition_mode == "absent":
            pass

        # 1. Process scalar/simple fields
        for path, f in fmap.items():
            if path.startswith("@"):
                continue

            # Skip helper fields marked emit: never
            if f.get("emit") == "never":
                continue

            # Skip explode_columns if mode is not named
            if path == "source_config.explode_columns" and explode_mode != "named":
                continue
            # Skip partition_columns if mode is not named
            if path == "target_config.partition_columns" and partition_mode != "named":
                continue

            # Check visibility
            vis_pred = f.get("visible_when")
            if vis_pred and not evaluate_predicate(vis_pred, context):
                continue

            widget = f.get("widget", "text")
            if widget in ("kv", "repeat", "note"):
                continue

            val = self.resolve_field_value(f, context)
            if val is None or val == "":
                continue

            coerced = self._coerce_field_value(val, f)
            if coerced is not None and coerced != "" and coerced != []:
                emit_path = f.get("emit_path", path)
                self._set_dotted_path(flow_out, emit_path, coerced)

        # 2. Process KV fields
        for path, kv_list in kvs_dict.items():
            if path.startswith("@"):
                continue
            f = fmap.get(path, {})
            vis_pred = f.get("visible_when")
            if vis_pred and not evaluate_predicate(vis_pred, context):
                continue

            kv_obj = {row[0]: row[1] for row in kv_list if len(row) >= 2 and row[0]}
            if kv_obj:
                emit_path = f.get("emit_path", path)
                self._set_dotted_path(flow_out, emit_path, kv_obj)

        # 3. Process Repeat fields
        for path, items in reps_dict.items():
            if path.startswith("@"):
                continue
            f = fmap.get(path, {})
            vis_pred = f.get("visible_when")
            if vis_pred and not evaluate_predicate(vis_pred, context):
                continue

            field_children = f.get("fields", [])
            serialized_rows = [
                self._serialize_repeat_item(row, field_children, context, flow_kind)
                for row in items
            ]
            # Filter out empty rows
            serialized_rows = [r for r in serialized_rows if r]

            if not serialized_rows:
                continue

            # Special transform: group_into_parent for decrypted_columns
            if path == "decrypted_columns" and flow_kind == "transformation":
                # Group decrypted_columns into matching source_inputs[] by input_name
                inputs_list = flow_out.get("source_inputs", [])
                for dec_row in serialized_rows:
                    input_name = dec_row.pop("input_name", None)
                    if input_name:
                        matched = False
                        for inp in inputs_list:
                            if inp.get("input_name") == input_name:
                                inp.setdefault("decrypted_columns", []).append(dec_row)
                                matched = True
                                break
                        if not matched:
                            inputs_list.append({"input_name": input_name, "decrypted_columns": [dec_row]})
                flow_out["source_inputs"] = inputs_list
            else:
                emit_path = f.get("emit_path", path)
                self._set_dotted_path(flow_out, emit_path, serialized_rows)

        return self._prune_empty_blocks(flow_out)

    def _prune_empty_blocks(self, d: Any) -> Any:
        """Recursively prune empty dictionaries from output."""
        if not isinstance(d, dict):
            return d
        clean = {}
        for k, v in d.items():
            if isinstance(v, dict):
                sub = self._prune_empty_blocks(v)
                if sub:
                    clean[k] = sub
            elif isinstance(v, list):
                sub_list = [self._prune_empty_blocks(x) for x in v]
                clean[k] = [x for x in sub_list if x is not None and (not isinstance(x, dict) or len(x) > 0)]
            elif v is not None and v != "":
                clean[k] = v
        return clean

    def serialize_spec(self, spec_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Assemble full top-level spec dictionary from SpecDoc."""
        spec_out: Dict[str, Any] = {}
        root = spec_doc.get("root", {})
        root_v = root.get("v", {})
        root_kvs = root.get("kvs", {})
        root_reps = root.get("reps", {})

        # 1. Root fields
        group_id = root_v.get("@dataflow_group_id") or root_v.get("dataflow_group_id")
        if group_id:
            spec_out["dataflow_group_id"] = group_id

        # Pipeline parameters
        pp_list = root_kvs.get("@pipeline_parameters") or root_kvs.get("pipeline_parameters") or []
        pp_dict = {r[0]: r[1] for r in pp_list if len(r) >= 2 and r[0]}
        if pp_dict:
            spec_out["pipeline_parameters"] = pp_dict

        # Spark config
        sc_list = root_kvs.get("@spark_config") or root_kvs.get("spark_config") or []
        sc_dict = {r[0]: r[1] for r in sc_list if len(r) >= 2 and r[0]}
        if sc_dict:
            spec_out["spark_config"] = sc_dict

        # 2. Collections
        ing_flows = spec_doc.get("ingestion_flows", [])
        if ing_flows:
            serialized_ing = [self.serialize_flow(f, "ingestion", root) for f in ing_flows]
            serialized_ing = [f for f in serialized_ing if f]
            if serialized_ing:
                spec_out["ingestion_flows"] = serialized_ing

        trn_flows = spec_doc.get("transformation_flows", [])
        if trn_flows:
            serialized_trn = [self.serialize_flow(f, "transformation", root) for f in trn_flows]
            serialized_trn = [f for f in serialized_trn if f]
            if serialized_trn:
                spec_out["transformation_flows"] = serialized_trn

        rec_flows = spec_doc.get("reconciliation_flows", [])
        if rec_flows:
            serialized_rec = [self.serialize_flow(f, "reconciliation", root) for f in rec_flows]
            serialized_rec = [f for f in serialized_rec if f]
            if serialized_rec:
                spec_out["reconciliation_flows"] = serialized_rec

        # 3. Observability
        obs_enabled = root_v.get("@observability_enabled", False)
        if isinstance(obs_enabled, str):
            obs_enabled = obs_enabled.lower() in ("true", "1", "yes")

        if obs_enabled:
            obs_raw_list = root_reps.get("@observability") or root_reps.get("observability") or []
            obs_field_def = self.reg.field_maps.get("observability", {}).get("@observability", {})
            obs_child_fields = obs_field_def.get("fields", [])

            serialized_obs = [
                self._serialize_repeat_item(row, obs_child_fields, {"root": root}, "observability")
                for row in obs_raw_list
            ]
            serialized_obs = [o for o in serialized_obs if o]
            if serialized_obs:
                spec_out["observability"] = serialized_obs

        return spec_out

    def render(self, spec_doc: Dict[str, Any], fmt: str = "json") -> Tuple[str, int]:
        """Render spec document to string (JSON or YAML) and return (content, byte_count)."""
        spec_dict = self.serialize_spec(spec_doc)
        if fmt.lower() in ("yaml", "yml"):
            content = yaml.dump(spec_dict, sort_keys=False, default_flow_style=False)
        else:
            content = json.dumps(spec_dict, indent=2)
        return content, len(content.encode("utf-8"))
