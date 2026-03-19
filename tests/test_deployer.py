"""Tests for Databricks deployer — all SDK calls mocked."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from metricviews.deployer import build_ddl, deploy_directory, deploy_file

_VALID_YAML = """version: "1.1"
source: cat.sch.tbl
dimensions:
  - name: D1
    expr: "col1"
measures:
  - name: M1
    expr: "SUM(col2)"
"""


class TestBuildDdl:
    def test_backtick_quotes_identifiers(self):
        ddl = build_ddl("yaml content", "my_cat", "my_sch", "my_view")
        assert "`my_cat`.`my_sch`.`my_view`" in ddl

    def test_wraps_yaml_in_dollar_quotes(self):
        ddl = build_ddl("version: 1.1\nsource: x", "c", "s", "v")
        assert "AS $$" in ddl
        assert "version: 1.1" in ddl
        assert ddl.endswith("$$")

    def test_uses_create_or_replace(self):
        ddl = build_ddl("yaml", "c", "s", "v")
        assert ddl.startswith("CREATE OR REPLACE VIEW")

    def test_rejects_dollar_quote_in_yaml_content(self):
        """Security: crafted YAML with $$ could escape DDL quoting."""
        with pytest.raises(ValueError, match=r"\$\$"):
            build_ddl("$$; DROP VIEW x; $$", "c", "s", "v")

    def test_rejects_backtick_in_catalog(self):
        """Security: backtick in identifier could break quoting."""
        with pytest.raises(ValueError, match="Invalid catalog"):
            build_ddl("yaml", "my`cat", "s", "v")

    def test_rejects_backtick_in_schema(self):
        with pytest.raises(ValueError, match="Invalid schema"):
            build_ddl("yaml", "c", "my`sch", "v")

    def test_rejects_backtick_in_view_name(self):
        with pytest.raises(ValueError, match="Invalid view_name"):
            build_ddl("yaml", "c", "s", "my`view")

    def test_allows_hyphenated_identifiers(self):
        """Real-world Databricks catalogs can have hyphens (e.g. prod-us-east)."""
        ddl = build_ddl("yaml", "prod-us-east", "my_sch", "my_view")
        assert "`prod-us-east`" in ddl

    def test_rejects_semicolon_in_identifier(self):
        with pytest.raises(ValueError, match="Invalid catalog"):
            build_ddl("yaml", "cat;DROP", "s", "v")


class TestDeployFile:
    def test_dry_run_returns_sql_without_executing(self, tmp_path: Path):
        f = tmp_path / "test.yaml"
        f.write_text(_VALID_YAML)
        client = MagicMock()

        result = deploy_file(client, f, "cat", "sch", "wh123", dry_run=True)

        assert result.status == "dry_run"
        assert "cat" in result.sql
        assert result.view_fqn == "cat.sch.test"
        client.statement_execution.execute_statement.assert_not_called()

    def test_successful_deploy(self, tmp_path: Path):
        from databricks.sdk.service.sql import StatementState

        f = tmp_path / "test.yaml"
        f.write_text(_VALID_YAML)

        response = MagicMock()
        response.status.state = StatementState.SUCCEEDED

        client = MagicMock()
        client.statement_execution.execute_statement.return_value = response

        result = deploy_file(client, f, "cat", "sch", "wh123")
        assert result.status == "success"

    def test_failed_deploy(self, tmp_path: Path):
        f = tmp_path / "test.yaml"
        f.write_text(_VALID_YAML)

        client = MagicMock()
        client.statement_execution.execute_statement.side_effect = Exception("boom")

        result = deploy_file(client, f, "cat", "sch", "wh123")
        assert result.status == "failed"
        assert "boom" in result.error  # type: ignore[operator]

    def test_custom_view_name(self, tmp_path: Path):
        f = tmp_path / "test.yaml"
        f.write_text(_VALID_YAML)
        client = MagicMock()

        result = deploy_file(
            client, f, "cat", "sch", "wh123", view_name="custom_name", dry_run=True
        )
        assert result.view_fqn == "cat.sch.custom_name"

    def test_non_succeeded_state_returns_failed(self, tmp_path: Path):
        """SDK call succeeds but returns FAILED state (e.g. SQL syntax error)."""
        from databricks.sdk.service.sql import StatementState

        f = tmp_path / "test.yaml"
        f.write_text(_VALID_YAML)

        response = MagicMock()
        response.status.state = StatementState.FAILED
        response.status.error = "PARSE_SYNTAX_ERROR"

        client = MagicMock()
        client.statement_execution.execute_statement.return_value = response

        result = deploy_file(client, f, "cat", "sch", "wh123")
        assert result.status == "failed"
        assert "PARSE_SYNTAX_ERROR" in result.error  # type: ignore[operator]

    def test_failed_state_with_no_error_detail(self, tmp_path: Path):
        """SDK returns FAILED state but error field is None — must not show 'None'."""
        from databricks.sdk.service.sql import StatementState

        f = tmp_path / "test.yaml"
        f.write_text(_VALID_YAML)

        response = MagicMock()
        response.status.state = StatementState.FAILED
        response.status.error = None

        client = MagicMock()
        client.statement_execution.execute_statement.return_value = response

        result = deploy_file(client, f, "cat", "sch", "wh123")
        assert result.status == "failed"
        assert result.error != "None"
        assert "did not succeed" in result.error  # type: ignore[operator]

    def test_deploy_file_validates_before_deploying(self, tmp_path: Path):
        """deploy_file must validate — invalid YAML should fail without executing."""
        bad = tmp_path / "bad.yaml"
        bad.write_text(
            'version: "1.1"\nsource: cat.sch.tbl\ndimensions:\n  - name: D\n    expr: "c"'
        )
        client = MagicMock()

        result = deploy_file(client, bad, "cat", "sch", "wh123")
        assert result.status == "failed"
        assert "Validation" in result.error  # type: ignore[operator]
        client.statement_execution.execute_statement.assert_not_called()


class TestDeployDirectory:
    def test_empty_directory_returns_failure(self, tmp_path: Path):
        """Empty directory should not silently succeed."""
        client = MagicMock()
        results = deploy_directory(client, tmp_path, "cat", "sch", "wh123")
        assert len(results) == 1
        assert results[0].status == "failed"
        assert "No YAML files" in results[0].error  # type: ignore[operator]

    def test_skips_files_with_validation_errors(self, tmp_path: Path):
        # Invalid YAML — missing measures
        bad = tmp_path / "bad.yaml"
        bad.write_text(
            'version: "1.1"\nsource: cat.sch.tbl\ndimensions:\n  - name: D\n    expr: "c"'
        )

        client = MagicMock()
        results = deploy_directory(client, tmp_path, "cat", "sch", "wh123")
        assert len(results) == 1
        assert results[0].status == "failed"
        assert "Validation" in results[0].error  # type: ignore[operator]
