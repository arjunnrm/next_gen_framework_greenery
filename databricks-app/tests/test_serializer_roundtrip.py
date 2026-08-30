import json
from pathlib import Path
import pytest
from server.core.deserializer import SpecDeserializer
from server.core.registry import RegistryManager
from server.core.serializer import SpecSerializer


def test_reference_template_roundtrip():
    reg = RegistryManager()
    ser = SpecSerializer(reg)
    deser = SpecDeserializer(reg)

    template_path = Path(__file__).parent.parent / "templates" / "pipeline_onboarding_template.json"
    with open(template_path, "r", encoding="utf-8") as f:
        raw_spec = json.load(f)

    # 1. Deserialize raw spec into SpecDoc
    spec_doc = deser.deserialize_spec(raw_spec)

    # Assert root parsed
    assert spec_doc["root"]["v"].get("dataflow_group_id") == "dfg_template_example"
    assert len(spec_doc["ingestion_flows"]) == 4
    assert len(spec_doc["transformation_flows"]) == 12
    assert len(spec_doc["reconciliation_flows"]) == 2
    assert len(spec_doc["root"]["reps"].get("@observability", [])) == 2

    # 2. Serialize SpecDoc back to framework spec dict
    exported_spec = ser.serialize_spec(spec_doc)

    assert exported_spec["dataflow_group_id"] == "dfg_template_example"
    assert len(exported_spec["ingestion_flows"]) == 4
    assert len(exported_spec["transformation_flows"]) == 12
    assert len(exported_spec["reconciliation_flows"]) == 2
    assert len(exported_spec["observability"]) == 2

    # 3. Roundtrip idempotence check
    roundtrip_doc = deser.deserialize_spec(exported_spec)
    re_exported_spec = ser.serialize_spec(roundtrip_doc)

    assert re_exported_spec == exported_spec
