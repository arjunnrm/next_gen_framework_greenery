"""Unit tests for ingestion/schema_config.py.

``TestResolveSchemaConfigPath``/``TestLoadSchemaConfig`` are pure/file-based -- no Spark session
needed (``dbutils=None`` is safe: every path under test is a real local file, and
``read_raw_spec_text``'s ``dbutils.fs.head`` fallback is never reached -- same pattern as
tests/unit/test_spec_loader.py). ``TestApplySchemaConfig`` uses the live ``spark`` fixture
(Databricks Connect) since it exercises real DataFrame transforms -- no table is created,
matching ``test_column_ordering.py``'s same precedent for DataFrame-transform-only tests living
in ``tests/unit/`` rather than ``tests/integration/``.
"""

import json
import time

import pytest
import yaml

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError
from NextGen_Metadata_Framework.lakeflow_framework.ingestion.schema_config import (
    apply_schema_config,
    load_schema_config,
    resolve_schema_config_path,
)

_VALID_SCHEMA_CONFIG = {
    "columns": [
        {"source_name": "CustomerID", "target_name": "customer_id", "data_type": "STRING", "nullable": False, "comment": "PK"},
        {"source_name": "SignupDate", "target_name": "signup_date", "data_type": "DATE"},
    ]
}


class TestResolveSchemaConfigPath:
    def test_exact_file_path_returned_unchanged(self, tmp_path):
        f = tmp_path / "schema.json"
        f.write_text("{}", encoding="utf-8")
        assert resolve_schema_config_path(str(f)) == str(f)

    def test_directory_resolves_to_most_recently_modified_file(self, tmp_path):
        older = tmp_path / "schema_v1.json"
        older.write_text("{}", encoding="utf-8")
        time.sleep(0.05)
        newer = tmp_path / "schema_v2.json"
        newer.write_text("{}", encoding="utf-8")
        assert resolve_schema_config_path(str(tmp_path)) == str(newer)

    def test_directory_tie_broken_by_lexicographically_largest_filename(self, tmp_path):
        a = tmp_path / "schema_a.json"
        b = tmp_path / "schema_b.json"
        a.write_text("{}", encoding="utf-8")
        b.write_text("{}", encoding="utf-8")
        import os

        same_time = os.path.getmtime(a)
        os.utime(b, (same_time, same_time))
        assert resolve_schema_config_path(str(tmp_path)) == str(b)

    def test_nonexistent_path_raises(self, tmp_path):
        with pytest.raises(FrameworkConfigError, match="does not exist"):
            resolve_schema_config_path(str(tmp_path / "does_not_exist.json"))

    def test_empty_directory_raises(self, tmp_path):
        with pytest.raises(FrameworkConfigError, match="contains no files"):
            resolve_schema_config_path(str(tmp_path))


class TestLoadSchemaConfig:
    def test_loads_and_parses_json_file(self, tmp_path):
        f = tmp_path / "schema.json"
        f.write_text(json.dumps(_VALID_SCHEMA_CONFIG), encoding="utf-8")
        result = load_schema_config(None, str(f))
        assert result == _VALID_SCHEMA_CONFIG

    def test_loads_and_parses_yaml_file(self, tmp_path):
        f = tmp_path / "schema.yaml"
        f.write_text(yaml.safe_dump(_VALID_SCHEMA_CONFIG), encoding="utf-8")
        result = load_schema_config(None, str(f))
        assert result == _VALID_SCHEMA_CONFIG

    def test_resolves_latest_file_when_given_a_directory(self, tmp_path):
        (tmp_path / "schema_v1.json").write_text(json.dumps({"columns": [{"source_name": "old"}]}), encoding="utf-8")
        time.sleep(0.05)
        (tmp_path / "schema_v2.json").write_text(json.dumps(_VALID_SCHEMA_CONFIG), encoding="utf-8")
        result = load_schema_config(None, str(tmp_path))
        assert result == _VALID_SCHEMA_CONFIG

    def test_catalog_env_placeholders_substituted(self, tmp_path):
        f = tmp_path / "schema.json"
        f.write_text(json.dumps({"columns": [{"source_name": "x", "comment": "in {{env}} for {{catalog}}"}]}), encoding="utf-8")
        result = load_schema_config(None, str(f), catalog="poc", environment="dev")
        assert result["columns"][0]["comment"] == "in dev for poc"

    def test_missing_columns_key_raises(self, tmp_path):
        f = tmp_path / "schema.json"
        f.write_text(json.dumps({"not_columns": []}), encoding="utf-8")
        with pytest.raises(FrameworkConfigError, match="top-level 'columns' array"):
            load_schema_config(None, str(f))

    def test_empty_columns_array_raises(self, tmp_path):
        f = tmp_path / "schema.json"
        f.write_text(json.dumps({"columns": []}), encoding="utf-8")
        with pytest.raises(FrameworkConfigError, match="non-empty array"):
            load_schema_config(None, str(f))

    def test_column_entry_missing_source_name_raises(self, tmp_path):
        f = tmp_path / "schema.json"
        f.write_text(json.dumps({"columns": [{"target_name": "x"}]}), encoding="utf-8")
        with pytest.raises(FrameworkConfigError, match="missing required key 'source_name'"):
            load_schema_config(None, str(f))

    def test_malformed_json_raises(self, tmp_path):
        f = tmp_path / "schema.json"
        f.write_text("{not valid json", encoding="utf-8")
        with pytest.raises(FrameworkConfigError):
            load_schema_config(None, str(f))


class TestApplySchemaConfig:
    def test_renames_and_casts_configured_columns(self, spark):
        df = spark.createDataFrame([("C001", "42")], ["CustomerID", "Age"])
        schema_config = {"columns": [{"source_name": "CustomerID", "target_name": "customer_id", "data_type": "STRING"}, {"source_name": "Age", "target_name": "age", "data_type": "INT"}]}
        result = apply_schema_config(df, schema_config)
        assert result.columns == ["customer_id", "age"]
        assert dict(result.dtypes)["age"] == "int"

    def test_omitted_target_name_defaults_to_source_name(self, spark):
        df = spark.createDataFrame([("C001",)], ["CustomerID"])
        schema_config = {"columns": [{"source_name": "CustomerID", "data_type": "STRING"}]}
        result = apply_schema_config(df, schema_config)
        assert result.columns == ["CustomerID"]

    def test_omitted_data_type_renames_without_casting(self, spark):
        df = spark.createDataFrame([("C001",)], ["CustomerID"])
        schema_config = {"columns": [{"source_name": "CustomerID", "target_name": "customer_id"}]}
        result = apply_schema_config(df, schema_config)
        assert result.columns == ["customer_id"]

    def test_unconfigured_columns_pass_through_unchanged(self, spark):
        df = spark.createDataFrame([("C001", "Alice", "GOLD")], ["CustomerID", "name", "tier"])
        schema_config = {"columns": [{"source_name": "CustomerID", "target_name": "customer_id"}]}
        result = apply_schema_config(df, schema_config)
        assert result.columns == ["customer_id", "name", "tier"]

    def test_comment_attached_as_column_metadata(self, spark):
        df = spark.createDataFrame([("C001",)], ["CustomerID"])
        schema_config = {"columns": [{"source_name": "CustomerID", "target_name": "customer_id", "comment": "Primary key"}]}
        result = apply_schema_config(df, schema_config)
        field = result.schema["customer_id"]
        assert field.metadata.get("comment") == "Primary key"

    def test_data_is_preserved_after_cast_and_rename(self, spark):
        df = spark.createDataFrame([("C001", "42")], ["CustomerID", "Age"])
        schema_config = {"columns": [{"source_name": "Age", "target_name": "age", "data_type": "INT"}]}
        result = apply_schema_config(df, schema_config)
        row = result.collect()[0]
        assert row["age"] == 42

    def test_missing_source_column_raises(self, spark):
        df = spark.createDataFrame([("C001",)], ["CustomerID"])
        schema_config = {"columns": [{"source_name": "DoesNotExist", "target_name": "x"}]}
        with pytest.raises(FrameworkConfigError, match="not present on the incoming DataFrame"):
            apply_schema_config(df, schema_config)
