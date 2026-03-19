"""Tests for YAML metric view validator."""

from pathlib import Path

from metricviews.validator import validate_directory, validate_file

FIXTURES = Path(__file__).parent / "fixtures"


class TestValidateFile:
    def test_valid_sample_orders(self):
        errors = validate_file(FIXTURES / "sample_orders.yaml")
        assert not any(e.severity == "error" for e in errors)

    def test_valid_star_schema(self):
        errors = validate_file(FIXTURES / "star_schema_with_joins.yaml")
        assert not any(e.severity == "error" for e in errors)

    def test_invalid_missing_measures(self):
        errors = validate_file(FIXTURES / "invalid_missing_measures.yaml")
        assert any(e.severity == "error" for e in errors)

    def test_window_measures_emit_warning(self):
        errors = validate_file(FIXTURES / "with_window_measures.yaml")
        warnings = [e for e in errors if e.severity == "warning"]
        assert any("window" in e.message.lower() for e in warnings)

    def test_materialization_emits_warning(self):
        errors = validate_file(FIXTURES / "with_materialization.yaml")
        warnings = [e for e in errors if e.severity == "warning"]
        assert any("materialization" in e.message.lower() for e in warnings)

    def test_nonexistent_file(self):
        errors = validate_file(FIXTURES / "does_not_exist.yaml")
        assert len(errors) == 1
        assert errors[0].severity == "error"

    def test_unquoted_on_key_gets_warning_not_crash(self):
        """Critical: unquoted 'on' in YAML becomes boolean True.
        Validator must detect this and emit a warning, NOT crash with
        a confusing Pydantic error."""
        errors = validate_file(FIXTURES / "unquoted_on_key.yaml")
        warnings = [e for e in errors if e.severity == "warning"]
        assert any("on" in e.message.lower() and "quoted" in e.message.lower() for e in warnings)
        # Must NOT have a Pydantic schema error about missing on/using
        schema_errors = [e for e in errors if "specify either" in e.message.lower()]
        assert len(schema_errors) == 0

    def test_placeholder_join_key_is_error(self, tmp_path):
        yaml_content = """version: "1.1"
source: cat.sch.tbl
joins:
  - name: dim
    source: cat.sch.dim
    "on": source.??? = dim.???
dimensions:
  - name: D1
    expr: "col1"
measures:
  - name: M1
    expr: "SUM(col2)"
"""
        f = tmp_path / "placeholder.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        assert any("???" in e.message for e in errors)

    def test_too_many_synonyms_is_error(self, tmp_path):
        yaml_content = """version: "1.1"
source: cat.sch.tbl
dimensions:
  - name: D1
    expr: "col1"
    synonyms: ["a","b","c","d","e","f","g","h","i","j","k"]
measures:
  - name: M1
    expr: "SUM(col2)"
"""
        f = tmp_path / "synonyms.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        assert any("synonym" in e.message.lower() for e in errors)

    def test_unknown_format_type_is_error(self, tmp_path):
        yaml_content = """version: "1.1"
source: cat.sch.tbl
dimensions:
  - name: D1
    expr: "col1"
    format:
      type: "invalid_type"
measures:
  - name: M1
    expr: "SUM(col2)"
"""
        f = tmp_path / "format.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        assert any("format type" in e.message.lower() for e in errors)

    def test_unsupported_version_is_error(self, tmp_path):
        yaml_content = """version: "2.0"
source: cat.sch.tbl
dimensions:
  - name: D1
    expr: "col1"
measures:
  - name: M1
    expr: "SUM(col2)"
"""
        f = tmp_path / "bad_version.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        assert any("version" in e.message.lower() and "2.0" in e.message for e in errors)

    def test_schema_error_messages_are_human_readable(self, tmp_path):
        """Pydantic errors must not expose input_value dumps or pydantic.dev URLs."""
        yaml_content = """version: "1.1"
source: cat.sch.tbl
dimensionss:
  - name: D1
    expr: "col1"
measures:
  - name: M1
    expr: "SUM(col2)"
"""
        f = tmp_path / "typo.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        schema_errors = [e for e in errors if e.severity == "error"]
        assert len(schema_errors) > 0
        for e in schema_errors:
            assert "input_value" not in e.message
            assert "pydantic.dev" not in e.message
            assert "input_type" not in e.message
        # Should mention the unknown field and the missing field
        combined = " ".join(e.message for e in schema_errors)
        assert "dimensionss" in combined
        assert "dimensions" in combined

    def test_non_fqn_source_emits_warning(self, tmp_path):
        yaml_content = """version: "1.1"
source: not_a_valid_source
dimensions:
  - name: D1
    expr: "col1"
measures:
  - name: M1
    expr: "SUM(col2)"
"""
        f = tmp_path / "bad_source.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        warnings = [e for e in errors if e.severity == "warning"]
        assert any("fully-qualified" in e.message for e in warnings)


class TestValidateDirectory:
    def test_validates_all_fixtures(self):
        errors = validate_directory(FIXTURES)
        # Should find errors (invalid_missing_measures) and warnings
        assert len(errors) > 0

    def test_empty_directory(self, tmp_path):
        errors = validate_directory(tmp_path)
        assert len(errors) == 1
        assert "No YAML" in errors[0].message
