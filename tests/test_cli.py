"""Tests for CLI — uses Click CliRunner, no real Databricks needed."""

from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from metricviews.cli import cli

FIXTURES = Path(__file__).parent / "fixtures"


class TestVersion:
    def test_version_flag(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "0.1.0" in result.output


class TestValidateCommand:
    def test_valid_file_returns_zero(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", str(FIXTURES / "sample_orders.yaml")])
        assert result.exit_code == 0

    def test_invalid_file_returns_nonzero(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "validate",
                str(FIXTURES / "invalid_missing_measures.yaml"),
            ],
        )
        assert result.exit_code != 0

    def test_strict_mode_fails_on_warnings(self):
        runner = CliRunner()
        # with_window_measures.yaml emits experimental warnings
        result = runner.invoke(
            cli,
            [
                "validate",
                str(FIXTURES / "with_window_measures.yaml"),
                "--strict",
            ],
        )
        assert result.exit_code != 0

    def test_directory_validation(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", str(FIXTURES)])
        # Fixtures dir has invalid files, so should fail
        assert result.exit_code != 0


class TestDeployCommand:
    def test_dry_run_prints_sql(self, tmp_path: Path):
        f = tmp_path / "test.yaml"
        f.write_text((FIXTURES / "sample_orders.yaml").read_text())

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "deploy",
                str(f),
                "--catalog",
                "dev",
                "--schema",
                "metrics",
                "--warehouse-id",
                "wh123",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0
        assert "dry-run" in result.output.lower() or "dry_run" in result.output.lower()


class TestGenerateCommand:
    def test_rejects_bad_source_fqn(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "generate",
                "--source",
                "not_valid",
                "--output",
                "/tmp/test.yaml",
            ],
        )
        assert result.exit_code != 0
        assert "catalog.schema.table" in result.output or "Bad Parameter" in result.output


class TestSdkErrorWrapping:
    @patch("metricviews.cli.introspector")
    def test_auth_failure_shows_friendly_message(self, mock_intro):
        mock_intro.create_client.side_effect = Exception("401 Unauthorized: InvalidAccessToken")
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "inspect",
                "--source",
                "cat.sch.tbl",
            ],
        )
        assert result.exit_code != 0
        assert "Authentication failed" in result.output

    @patch("metricviews.cli.introspector")
    def test_connection_error_shows_friendly_message(self, mock_intro):
        err = ConnectionError("Failed to connect")
        mock_intro.create_client.side_effect = err
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "inspect",
                "--source",
                "cat.sch.tbl",
            ],
        )
        assert result.exit_code != 0
        assert "Cannot reach" in result.output or "API error" in result.output
