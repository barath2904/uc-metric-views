"""Shared pytest fixtures for uc-metric-views tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from metricviews.models import DimensionDef, MeasureDef, MetricViewSpec

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture()
def fixtures_dir() -> Path:
    """Path to the tests/fixtures/ directory."""
    return FIXTURES_DIR


@pytest.fixture()
def minimal_spec() -> MetricViewSpec:
    """Smallest valid MetricViewSpec — one dimension, one measure."""
    return MetricViewSpec(
        source="cat.sch.tbl",
        dimensions=[DimensionDef(name="D1", expr="col1")],
        measures=[MeasureDef(name="M1", expr="SUM(col2)")],
    )


@pytest.fixture()
def valid_yaml_path(tmp_path: Path) -> Path:
    """Write a minimal valid YAML file and return its path."""
    content = """\
version: "1.1"
source: cat.sch.tbl
dimensions:
  - name: D1
    expr: "col1"
measures:
  - name: M1
    expr: "SUM(col2)"
"""
    p = tmp_path / "valid.yaml"
    p.write_text(content)
    return p
