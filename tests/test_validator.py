"""Tests for YAML metric view validator."""

from pathlib import Path

import pytest

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

    def test_window_measures_emit_suggestion(self):
        errors = validate_file(FIXTURES / "with_window_measures.yaml")
        suggestions = [e for e in errors if e.severity == "suggestion"]
        assert any("window" in e.message.lower() for e in suggestions)

    def test_materialization_emits_suggestion(self):
        errors = validate_file(FIXTURES / "with_materialization.yaml")
        suggestions = [e for e in errors if e.severity == "suggestion"]
        assert any("materialization" in e.message.lower() for e in suggestions)

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

    def test_non_fqn_source_emits_suggestion(self, tmp_path):
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
        suggestions = [e for e in errors if e.severity == "suggestion"]
        assert any("fully-qualified" in e.message for e in suggestions)


class TestFriendlyPydanticErrors:
    """Pydantic errors must not expose raw internals (input_value, pydantic.dev URLs)."""

    def test_schema_errors_are_human_readable(self, tmp_path):
        yaml_content = """version: "1.1"
source: cat.sch.tbl
dimensionss:
  - name: D1
    expr: "col1"
measures:
  - name: M1
    expr: "SUM(col2)"
"""
        f = tmp_path / "typo_field.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        schema_errors = [e for e in errors if e.severity == "error"]
        assert len(schema_errors) > 0
        for e in schema_errors:
            assert "input_value" not in e.message
            assert "pydantic.dev" not in e.message
            assert "input_type" not in e.message
        combined = " ".join(e.message for e in schema_errors)
        assert "dimensionss" in combined
        assert "dimensions" in combined

    def test_empty_dimensions_gives_readable_error(self, tmp_path):
        yaml_content = """version: "1.1"
source: cat.sch.tbl
dimensions: []
measures:
  - name: M1
    expr: "SUM(col2)"
"""
        f = tmp_path / "empty_dims.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        errs = [e for e in errors if e.severity == "error"]
        assert len(errs) > 0
        assert not any("input_value" in e.message for e in errs)
        assert any("dimensions" in e.message.lower() for e in errs)

    def test_typo_in_dimension_field_is_error(self, tmp_path):
        """Unknown field inside a dimension was previously swallowed; must now be an error."""
        yaml_content = """version: "1.1"
source: cat.sch.tbl
dimensions:
  - name: D1
    expr: "col1"
    dispaly_name: "typo"
measures:
  - name: M1
    expr: "SUM(col2)"
"""
        f = tmp_path / "dim_typo.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        errs = [e for e in errors if e.severity == "error"]
        assert len(errs) > 0
        assert any("dispaly_name" in e.message or "unknown" in e.message.lower() for e in errs)


class TestSuggestionSeverity:
    """Informational findings should be 'suggestion', not 'warning'."""

    def test_experimental_window_is_suggestion(self):
        errors = validate_file(FIXTURES / "with_window_measures.yaml")
        suggestions = [e for e in errors if e.severity == "suggestion"]
        assert any("window" in e.message.lower() for e in suggestions)
        # must NOT be a warning
        warnings = [e for e in errors if e.severity == "warning"]
        assert not any("window" in e.message.lower() for e in warnings)

    def test_experimental_materialization_is_suggestion(self):
        errors = validate_file(FIXTURES / "with_materialization.yaml")
        suggestions = [e for e in errors if e.severity == "suggestion"]
        assert any("materialization" in e.message.lower() for e in suggestions)
        warnings = [e for e in errors if e.severity == "warning"]
        assert not any("materialization" in e.message.lower() for e in warnings)

    def test_non_fqn_source_is_suggestion(self, tmp_path):
        yaml_content = """version: "1.1"
source: not_fqn
dimensions:
  - name: D1
    expr: "col1"
measures:
  - name: M1
    expr: "SUM(col2)"
"""
        f = tmp_path / "non_fqn.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        suggestions = [e for e in errors if e.severity == "suggestion"]
        assert any("fully-qualified" in e.message for e in suggestions)
        warnings = [e for e in errors if e.severity == "warning"]
        assert not any("fully-qualified" in e.message for e in warnings)

    def test_missing_aggregate_is_suggestion(self, tmp_path):
        yaml_content = """version: "1.1"
source: cat.sch.tbl
dimensions:
  - name: D1
    expr: "col1"
measures:
  - name: M1
    expr: "raw_col"
"""
        f = tmp_path / "no_agg.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        suggestions = [e for e in errors if e.severity == "suggestion"]
        assert any("aggregate" in e.message.lower() for e in suggestions)
        warnings = [e for e in errors if e.severity == "warning"]
        assert not any("aggregate" in e.message.lower() for e in warnings)


class TestNewValidations:
    """The 14 validation gaps identified in v0.2.0 planning."""

    def test_join_source_not_fqn_emits_warning(self, tmp_path):
        yaml_content = """version: "1.1"
source: cat.sch.tbl
joins:
  - name: dim
    source: not_an_fqn
    "on": "source.id = dim.id"
dimensions:
  - name: D1
    expr: "col1"
measures:
  - name: M1
    expr: "SUM(col2)"
"""
        f = tmp_path / "bad_join_src.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        assert any("join" in e.message.lower() and "fully-qualified" in e.message for e in errors)

    def test_placeholder_in_using_is_error(self, tmp_path):
        yaml_content = """version: "1.1"
source: cat.sch.tbl
joins:
  - name: dim
    source: cat.sch.dim
    using: ["???"]
dimensions:
  - name: D1
    expr: "col1"
measures:
  - name: M1
    expr: "SUM(col2)"
"""
        f = tmp_path / "using_placeholder.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        assert any("???" in e.message for e in errors)

    def test_format_missing_type_key_is_error(self, tmp_path):
        yaml_content = """version: "1.1"
source: cat.sch.tbl
dimensions:
  - name: D1
    expr: "col1"
    format:
      precision: 2
measures:
  - name: M1
    expr: "SUM(col2)"
"""
        f = tmp_path / "format_no_type.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        errs = [e for e in errors if e.severity == "error"]
        assert any("format" in e.message.lower() and "type" in e.message.lower() for e in errs)

    def test_synonym_empty_string_is_error(self, tmp_path):
        yaml_content = """version: "1.1"
source: cat.sch.tbl
dimensions:
  - name: D1
    expr: "col1"
    synonyms: ["revenue", ""]
measures:
  - name: M1
    expr: "SUM(col2)"
"""
        f = tmp_path / "empty_synonym.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        errs = [e for e in errors if e.severity == "error"]
        assert any("synonym" in e.message.lower() and "empty" in e.message.lower() for e in errs)

    def test_duplicate_synonyms_is_warning(self, tmp_path):
        yaml_content = """version: "1.1"
source: cat.sch.tbl
dimensions:
  - name: D1
    expr: "col1"
    synonyms: ["revenue", "revenue"]
measures:
  - name: M1
    expr: "SUM(col2)"
"""
        f = tmp_path / "dup_synonym.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        assert any(
            "synonym" in e.message.lower() and "duplicate" in e.message.lower() for e in errors
        )

    def test_matview_references_nonexistent_dimension_is_error(self, tmp_path):
        yaml_content = """version: "1.1"
source: cat.sch.tbl
dimensions:
  - name: D1
    expr: "col1"
measures:
  - name: M1
    expr: "SUM(col2)"
materialization:
  schedule: "every 6 hours"
  materialized_views:
    - name: daily
      type: full
      dimensions: ["nonexistent_dim"]
"""
        f = tmp_path / "matview_bad_ref.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        errs = [e for e in errors if e.severity == "error"]
        assert any("nonexistent_dim" in e.message for e in errs)

    def test_fqn_with_hyphens_does_not_warn(self, tmp_path):
        """Catalog/schema/table names with hyphens are valid Databricks identifiers."""
        yaml_content = """version: "1.1"
source: my-catalog.my-schema.fct_orders
dimensions:
  - name: D1
    expr: "col1"
measures:
  - name: M1
    expr: "SUM(col2)"
"""
        f = tmp_path / "hyphen_fqn.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        assert not any("fully-qualified" in e.message for e in errors)


class TestFormatTypeValidation:
    """Parameterized tests for all valid and invalid format types."""

    @pytest.mark.parametrize(
        "fmt_type", ["number", "currency", "percentage", "byte", "date", "date_time"]
    )
    def test_valid_format_types_accepted(self, tmp_path, fmt_type):
        yaml_content = f"""version: "1.1"
source: cat.sch.tbl
dimensions:
  - name: D1
    expr: "col1"
    format:
      type: "{fmt_type}"
measures:
  - name: M1
    expr: "SUM(col2)"
"""
        f = tmp_path / "format.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        assert not any("format type" in e.message.lower() for e in errors)

    @pytest.mark.parametrize("fmt_type", ["invalid", "money", "percent", "string", "time"])
    def test_invalid_format_types_rejected(self, tmp_path, fmt_type):
        yaml_content = f"""version: "1.1"
source: cat.sch.tbl
dimensions:
  - name: D1
    expr: "col1"
    format:
      type: "{fmt_type}"
measures:
  - name: M1
    expr: "SUM(col2)"
"""
        f = tmp_path / "format.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        errs = [e for e in errors if e.severity == "error"]
        assert any("format type" in e.message.lower() for e in errs)


class TestPydanticErrorFallback:
    """The else branch in _format_pydantic_errors for unknown Pydantic error types."""

    def test_unknown_pydantic_error_type_uses_msg(self, tmp_path):
        """Trigger a Pydantic error type not in _PYDANTIC_TYPE_MESSAGES.

        A model_validator 'value_error' with custom message tests the empty-string
        mapping path. A duplicate name triggers the model validator which produces
        a 'value_error' type — the dict maps it to '' so err['msg'] is used as-is.
        """
        yaml_content = """version: "1.1"
source: cat.sch.tbl
dimensions:
  - name: Same
    expr: "col1"
measures:
  - name: Same
    expr: "SUM(col2)"
"""
        f = tmp_path / "dup_names.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        errs = [e for e in errors if e.severity == "error"]
        assert len(errs) > 0
        # The value_error branch maps to "" so err["msg"] is used directly
        assert any("Duplicate" in e.message for e in errs)
        # Must still be human-readable
        for e in errs:
            assert "input_value" not in e.message
            assert "pydantic.dev" not in e.message


class TestValidateDirectory:
    def test_validates_all_fixtures(self):
        errors = validate_directory(FIXTURES)
        # Should find errors (invalid_missing_measures) and warnings
        assert len(errors) > 0

    def test_empty_directory(self, tmp_path):
        errors = validate_directory(tmp_path)
        assert len(errors) == 1
        assert "No YAML" in errors[0].message
