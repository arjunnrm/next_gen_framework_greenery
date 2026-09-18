"""
Validation Engine for the Metaflow Onboarding App.
Implements §12 Layer 1 (Registry-driven) + Layer 2 (Cross-field rules) + Secret Hygiene.
"""

import re
from typing import Any, Dict, List, Optional, Tuple, Union

from server.core.predicates import evaluate_predicate
from server.core.registry import RegistryManager


class SpecValidator:
    """Validates full specs and individual flow documents."""

    def __init__(self, registry_manager: Optional[RegistryManager] = None):
        if registry_manager is None:
            registry_manager = RegistryManager()
        self.reg = registry_manager

    def check_secret_hygiene(self, raw_data: Any, path_prefix: str = "") -> List[Dict[str, Any]]:
        """Scan raw dictionary/JSON depth-first for forbidden secret keys (§12.5)."""
        forbidden = set(self.reg.meta.get("forbidden_keys", []))
        errors: List[Dict[str, Any]] = []

        if isinstance(raw_data, dict):
            for k, v in raw_data.items():
                current_ptr = f"{path_prefix}/{k}" if path_prefix else f"/{k}"
                if k.lower() in forbidden:
                    errors.append({
                        "field_path": current_ptr,
                        "message": f"Plaintext secret field '{k}' is forbidden at pointer '{current_ptr}'. Use a Unity Catalog secret reference ('secret_catalog', 'secret_schema', 'secret_key') instead.",
                        "severity": "error",
                        "rule_id": "secret_hygiene_violation"
                    })
                errors.extend(self.check_secret_hygiene(v, current_ptr))
        elif isinstance(raw_data, list):
            for i, item in enumerate(raw_data):
                current_ptr = f"{path_prefix}/{i}"
                errors.extend(self.check_secret_hygiene(item, current_ptr))

        return errors

    def validate_spec(self, spec_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Run complete Layer 1, Layer 2, and Secret Hygiene validation on a SpecDoc.
        Returns:
          {
            "ok": bool,
            "errors": List[Dict[str, Any]],
            "warnings": List[Dict[str, Any]],
            "summary": { "total_errors": int, "total_warnings": int }
          }
        """
        errors: List[Dict[str, Any]] = []
        warnings: List[Dict[str, Any]] = []

        # 0. Secret Hygiene Scan
        secret_errors = self.check_secret_hygiene(spec_doc)
        errors.extend(secret_errors)

        # 1. Spec Root Validation
        root = spec_doc.get("root", {})
        root_v = root.get("v", {})
        root_kvs = root.get("kvs", {})

        group_id = root_v.get("@dataflow_group_id") or root_v.get("dataflow_group_id")
        if not group_id or not str(group_id).strip():
            errors.append({
                "field_path": "dataflow_group_id",
                "message": "dataflow_group_id is required.",
                "severity": "error",
                "rule_id": "required_field_missing"
            })
        elif not re.match(r"^[A-Za-z0-9_]+$", str(group_id)):
            errors.append({
                "field_path": "dataflow_group_id",
                "message": "dataflow_group_id must contain only alphanumeric characters and underscores.",
                "severity": "error",
                "rule_id": "dataflow_group_id_pattern"
            })

        # Spark config prefix check
        for row in (root_kvs.get("@spark_config") or root_kvs.get("spark_config") or []):
            if row and len(row) >= 1 and row[0]:
                key = str(row[0]).strip()
                if not key.startswith("spark."):
                    errors.append({
                        "field_path": "spark_config",
                        "message": f"spark_config key '{key}' must start with 'spark.'",
                        "severity": "error",
                        "rule_id": "spark_config_key_prefix"
                    })

        # 2. Flow Collections & Uniqueness Checks
        seen_dataflow_ids = set()
        seen_step_ids = set()
        seen_input_names = set()
        seen_recon_ids = set()
        seen_obs_ids = set()

        # Ingestion flows
        ing_flows = spec_doc.get("ingestion_flows", [])
        for f in ing_flows:
            df_id = f.get("v", {}).get("dataflow_id")
            if df_id:
                if df_id in seen_dataflow_ids:
                    errors.append({
                        "field_path": "dataflow_id",
                        "message": f"Duplicate dataflow_id '{df_id}' across ingestion flows.",
                        "severity": "error",
                        "rule_id": "unique_dataflow_id"
                    })
                seen_dataflow_ids.add(df_id)
            self._validate_flow_layer1(f, "ingestion", root, errors, warnings)
            self._validate_flow_layer2(f, "ingestion", root, errors, warnings)

        # Transformation flows
        trn_flows = spec_doc.get("transformation_flows", [])
        for f in trn_flows:
            step_id = f.get("v", {}).get("flow_step_id")
            if step_id:
                if step_id in seen_step_ids:
                    errors.append({
                        "field_path": "flow_step_id",
                        "message": f"Duplicate flow_step_id '{step_id}' across transformation flows.",
                        "severity": "error",
                        "rule_id": "unique_flow_step_id"
                    })
                seen_step_ids.add(step_id)

            # Check input_name uniqueness spec-wide
            for inp in (f.get("reps", {}).get("source_inputs") or []):
                in_name = inp.get("input_name")
                if in_name:
                    if in_name in seen_input_names:
                        errors.append({
                            "field_path": "source_inputs.input_name",
                            "message": f"Duplicate input_name '{in_name}' across spec.",
                            "severity": "error",
                            "rule_id": "unique_input_name_spec_wide"
                        })
                    seen_input_names.add(in_name)

            self._validate_flow_layer1(f, "transformation", root, errors, warnings)
            self._validate_flow_layer2(f, "transformation", root, errors, warnings)

        # Reconciliation flows
        rec_flows = spec_doc.get("reconciliation_flows", [])
        for f in rec_flows:
            rec_id = f.get("v", {}).get("reconciliation_id")
            if rec_id:
                if rec_id in seen_recon_ids:
                    errors.append({
                        "field_path": "reconciliation_id",
                        "message": f"Duplicate reconciliation_id '{rec_id}' across reconciliation flows.",
                        "severity": "error",
                        "rule_id": "unique_reconciliation_id"
                    })
                seen_recon_ids.add(rec_id)
            self._validate_flow_layer1(f, "reconciliation", root, errors, warnings)
            self._validate_flow_layer2(f, "reconciliation", root, errors, warnings)

        # Observability destinations
        obs_rows = root.get("reps", {}).get("@observability") or root.get("reps", {}).get("observability") or []
        for row in obs_rows:
            obs_id = row.get("id")
            if obs_id:
                if obs_id in seen_obs_ids:
                    errors.append({
                        "field_path": "observability.id",
                        "message": f"Duplicate destination id '{obs_id}' across observability destinations.",
                        "severity": "error",
                        "rule_id": "unique_observability_id"
                    })
                seen_obs_ids.add(obs_id)

        # 3. Spec-Level Cross-Field Rules (Layer 2)
        spec_context = {
            "v": root_v,
            "root": root,
            "@ingestion_flows": ing_flows,
            "@transformation_flows": trn_flows,
            "@reconciliation_flows": rec_flows
        }
        for rule in self.reg.rules:
            if rule.get("scope") == "spec":
                when_pred = rule.get("when")
                if when_pred and evaluate_predicate(when_pred, spec_context):
                    item = {
                        "field_path": rule.get("field_path"),
                        "message": rule.get("message"),
                        "severity": rule.get("severity", "error"),
                        "rule_id": rule.get("id")
                    }
                    if rule.get("severity") == "warning":
                        warnings.append(item)
                    else:
                        errors.append(item)

        return {
            "ok": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "summary": {
                "total_errors": len(errors),
                "total_warnings": len(warnings)
            }
        }

    def _validate_flow_layer1(self, flow_doc: Dict[str, Any], flow_kind: str, root_doc: Dict[str, Any], errors: List[Dict[str, Any]], warnings: List[Dict[str, Any]]) -> None:
        """Validate registry-level constraints (required-when-visible, patterns, options)."""
        fmap = self.reg.field_maps.get(flow_kind, {})
        v_dict = flow_doc.get("v", {})
        context = {
            "v": v_dict,
            "kvs": flow_doc.get("kvs", {}),
            "reps": flow_doc.get("reps", {}),
            "root": root_doc,
            "@__flow_kind": flow_kind
        }

        for path, f in fmap.items():
            if path.startswith("@"):
                continue

            vis_pred = f.get("visible_when")
            if vis_pred and not evaluate_predicate(vis_pred, context):
                continue

            val = v_dict.get(path)
            if val is None or val == "":
                val = f.get("default")

            # Required check
            if f.get("required"):
                if val is None or (isinstance(val, str) and not val.strip()) or (isinstance(val, list) and not val):
                    errors.append({
                        "field_path": path,
                        "message": f"'{path}' is required.",
                        "severity": "error",
                        "rule_id": "required_field_missing"
                    })

            # Enum validation
            if f.get("widget") == "select" and val and isinstance(val, str) and val.strip():
                opts = f.get("options", [])
                # Check options_when
                for entry in f.get("options_when", []):
                    if evaluate_predicate(entry.get("when"), context):
                        opts = entry.get("options", opts)
                        break
                if opts and val not in opts:
                    errors.append({
                        "field_path": path,
                        "message": f"Value '{val}' for '{path}' is not in allowed options {opts}.",
                        "severity": "error",
                        "rule_id": "invalid_enum_option"
                    })

            # Max items for liquid clustering
            if path == "target_config.liquid_clustering_columns" and val:
                items = [x.strip() for x in str(val).split(",") if x.strip()] if isinstance(val, str) else (val if isinstance(val, list) else [])
                if len(items) > 3:
                    errors.append({
                        "field_path": path,
                        "message": f"Liquid clustering supports at most 3 columns (provided {len(items)}).",
                        "severity": "error",
                        "rule_id": "liquid_clustering_max_3"
                    })

    def _validate_flow_layer2(self, flow_doc: Dict[str, Any], flow_kind: str, root_doc: Dict[str, Any], errors: List[Dict[str, Any]], warnings: List[Dict[str, Any]]) -> None:
        """Evaluate Layer 2 cross-field rules on a single flow document."""
        v_dict = flow_doc.get("v", {})
        context = {
            "v": v_dict,
            "kvs": flow_doc.get("kvs", {}),
            "reps": flow_doc.get("reps", {}),
            "root": root_doc,
            "@__flow_kind": flow_kind
        }

        for rule in self.reg.rules:
            if rule.get("scope") == "flow":
                when_pred = rule.get("when")
                if when_pred and evaluate_predicate(when_pred, context):
                    item = {
                        "field_path": rule.get("field_path"),
                        "message": rule.get("message"),
                        "severity": rule.get("severity", "error"),
                        "rule_id": rule.get("id")
                    }
                    if rule.get("severity") == "warning":
                        warnings.append(item)
                    else:
                        errors.append(item)
