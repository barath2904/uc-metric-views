# uc-metric-views v0.1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a working CLI tool (`ucm`) that generates, validates, and deploys Databricks metric view YAML definitions.

**Architecture:** Functions-over-classes, one-file-one-job. Pydantic models define the YAML spec schema. Heuristics classify columns. Generator scaffolds YAML per business concept. Validator runs 11 checks. Deployer wraps YAML in DDL and sends via SDK. See `docs/ARCHITECTURE.md` for full design.

**Tech Stack:** Python 3.10+, Pydantic 2, Click, PyYAML, databricks-sdk, pytest, ruff, mypy

---

## File Structure

```
Create: src/metricviews/__init__.py          — public API exports
Create: src/metricviews/py.typed             — PEP 561 type marker (empty file)
Create: src/metricviews/models.py            — Pydantic domain models
Create: src/metricviews/heuristics.py        — column classification logic
Create: src/metricviews/introspector.py      — Databricks SDK wrapper
Create: src/metricviews/generator.py         — YAML generation from table metadata
Create: src/metricviews/validator.py         — YAML validation (11 checks)
Create: src/metricviews/deployer.py          — DDL build + SDK deployment
Create: src/metricviews/cli.py               — Click CLI (4 commands + --version)
Create: tests/__init__.py                   — (empty)
Create: tests/conftest.py                  — shared pytest fixtures
Create: tests/test_models.py               — model validation tests
Create: tests/test_heuristics.py           — classification tests
Create: tests/test_introspector.py         — SDK mock tests
Create: tests/test_generator.py            — generation + YAML output tests
Create: tests/test_validator.py            — validation logic tests
Create: tests/test_deployer.py             — deployment mock tests
Create: tests/test_cli.py                  — Click CliRunner tests
Create: tests/fixtures/sample_orders.yaml
Create: tests/fixtures/star_schema_with_joins.yaml
Create: tests/fixtures/with_window_measures.yaml
Create: tests/fixtures/with_materialization.yaml
Create: tests/fixtures/invalid_missing_measures.yaml
Create: tests/fixtures/unquoted_on_key.yaml
Create: examples/README.md                 — usage guide for Databricks free tier
Create: examples/basic_single_table.yaml   — simplest metric view
Create: examples/star_schema_joins.yaml    — fact + dim tables
Create: examples/with_semantic_metadata.yaml — display_name, format, synonyms
Create: pyproject.toml                     — build config + dependencies
Create: README.md                          — open source users guide
Create: ROADMAP.md                         — future release plans
Create: LICENSE                            — MIT license
Create: .github/workflows/ci.yml          — PR lint + test + validate
Create: .github/workflows/deploy.yml      — merge-to-main deployment
```

---

## Task 1: Project Scaffold

**Files:**
- Create: `pyproject.toml`
- Create: `src/metricviews/__init__.py` (empty placeholder)
- Create: `src/metricviews/py.typed` (empty)
- Create: `tests/__init__.py` (empty)

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p src/metricviews tests/fixtures examples .github/workflows
```

- [ ] **Step 2: Create pyproject.toml**

```toml
[build-system]
requires = ["hatchling>=1.21,<2.0"]
build-backend = "hatchling.build"

[project]
name = "uc-metric-views"
version = "0.1.0"
description = "Generate, validate, and deploy Databricks Unity Catalog metric views"
readme = "README.md"
license = "MIT"
requires-python = ">=3.10"
authors = [{ name = "BK" }]
keywords = ["databricks", "metric-views", "unity-catalog", "semantic-layer"]
classifiers = [
    "Development Status :: 3 - Alpha",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "License :: OSI Approved :: MIT License",
    "Topic :: Database",
]

[project.urls]
Homepage = "https://github.com/bk/uc-metric-views"
Source = "https://github.com/bk/uc-metric-views"
Issues = "https://github.com/bk/uc-metric-views/issues"

dependencies = [
    "databricks-sdk>=0.30.0,<1.0",
    "pydantic>=2.0,<3.0",
    "click>=8.0,<9.0",
    "pyyaml>=6.0,<7.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0,<9.0",
    "pytest-cov>=5.0,<7.0",
    "ruff>=0.8.0,<1.0",
    "mypy>=1.10,<2.0",
]

[project.scripts]
ucm = "metricviews.cli:cli"

[tool.ruff]
target-version = "py310"
line-length = 100

[tool.ruff.lint]
select = [
    "E",      # pycodestyle errors
    "F",      # pyflakes
    "W",      # pycodestyle warnings
    "I",      # isort (import sorting)
    "UP",     # pyupgrade (modern Python syntax)
    "B",      # flake8-bugbear (common bugs)
    "SIM",    # flake8-simplify
    "RET",    # flake8-return (dead code after returns)
    "PERF",   # perflint (performance anti-patterns)
    "RUF",    # ruff-specific rules
]

[tool.ruff.lint.per-file-ignores]
"tests/**" = ["S101"]

[tool.mypy]
python_version = "3.10"
strict = true

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "--cov=metricviews --cov-report=term-missing"

[tool.coverage.run]
source = ["metricviews"]

[tool.coverage.report]
show_missing = true
fail_under = 80
```

- [ ] **Step 3: Create empty marker files**

```bash
touch src/metricviews/__init__.py
touch src/metricviews/py.typed
touch tests/__init__.py
```

- [ ] **Step 3b: Create tests/conftest.py with shared fixtures**

File: `tests/conftest.py`

```python
"""Shared pytest fixtures for uc-metric-views tests."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock
from metricviews.models import DiscoveredTable, DiscoveredColumn


@pytest.fixture()
def source_table() -> DiscoveredTable:
    """Standard fact table for generation/inspection tests."""
    return DiscoveredTable(
        catalog="cat", schema_name="sch", table_name="fct_orders",
        columns=[
            DiscoveredColumn(name="order_id", type_name="BIGINT"),
            DiscoveredColumn(name="order_date", type_name="DATE"),
            DiscoveredColumn(name="total_amount", type_name="DECIMAL(10,2)"),
            DiscoveredColumn(name="_etl_loaded_at", type_name="TIMESTAMP"),
        ],
    )


@pytest.fixture()
def dim_table() -> DiscoveredTable:
    """Standard dimension table for join tests."""
    return DiscoveredTable(
        catalog="cat", schema_name="sch", table_name="dim_customer",
        columns=[
            DiscoveredColumn(name="customer_id", type_name="BIGINT"),
            DiscoveredColumn(name="customer_name", type_name="STRING"),
        ],
    )


@pytest.fixture()
def mock_workspace_client() -> MagicMock:
    """Pre-configured mock of databricks.sdk.WorkspaceClient."""
    return MagicMock()
```

- [ ] **Step 4: Create LICENSE file**

Copy the existing `LICENSE` from the repo root (already created at initial commit). If missing, create MIT license text with `Copyright 2026 Barath Badrachalam Kannan`.

- [ ] **Step 5: Install in dev mode and verify**

```bash
pip install -e ".[dev]"
```

Expected: installs successfully, `ucm` command exists (will fail with import error — that's fine).

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml src/ tests/__init__.py LICENSE
git commit -m "chore: init project scaffold and pyproject.toml"
```

---

## Task 2: Domain Models

**Files:**
- Create: `src/metricviews/models.py`
- Create: `tests/test_models.py`

- [ ] **Step 1: Write failing tests**

File: `tests/test_models.py`

```python
"""Tests for Pydantic domain models."""
import pytest
import pydantic
from metricviews.models import (
    MetricViewSpec, DimensionDef, MeasureDef, JoinDef,
    DiscoveredColumn, DiscoveredTable, DeployResult,
    ColumnRole, WindowSpec, MaterializationConfig, MaterializedViewDef,
)


class TestMetricViewSpec:
    def test_minimal_valid_spec(self):
        spec = MetricViewSpec(
            source="cat.sch.tbl",
            dimensions=[DimensionDef(name="D1", expr="col1")],
            measures=[MeasureDef(name="M1", expr="SUM(col2)")],
        )
        assert spec.version == "1.1"
        assert spec.source == "cat.sch.tbl"

    def test_requires_at_least_one_dimension(self):
        with pytest.raises(pydantic.ValidationError):
            MetricViewSpec(
                source="cat.sch.tbl",
                dimensions=[],
                measures=[MeasureDef(name="M1", expr="SUM(col2)")],
            )

    def test_requires_at_least_one_measure(self):
        with pytest.raises(pydantic.ValidationError):
            MetricViewSpec(
                source="cat.sch.tbl",
                dimensions=[DimensionDef(name="D1", expr="col1")],
                measures=[],
            )

    def test_rejects_duplicate_names(self):
        with pytest.raises(ValueError, match="Duplicate"):
            MetricViewSpec(
                source="cat.sch.tbl",
                dimensions=[DimensionDef(name="Revenue", expr="col1")],
                measures=[MeasureDef(name="Revenue", expr="SUM(col2)")],
            )

    def test_rejects_unknown_keys(self):
        """ConfigDict(extra='forbid') catches YAML typos like 'dimnsions'."""
        with pytest.raises(pydantic.ValidationError, match="extra"):
            MetricViewSpec(
                source="cat.sch.tbl",
                dimensions=[DimensionDef(name="D1", expr="col1")],
                measures=[MeasureDef(name="M1", expr="SUM(col2)")],
                unknown_field="oops",
            )

    def test_full_spec_with_optionals(self):
        spec = MetricViewSpec(
            source="cat.sch.tbl",
            comment="Test metric view",
            filter="col1 > 0",
            dimensions=[DimensionDef(
                name="D1", expr="col1",
                display_name="Dim One",
                format={"type": "date", "date_format": "year_month_day"},
                synonyms=["dim one", "first dim"],
            )],
            measures=[MeasureDef(
                name="M1", expr="SUM(col2)",
                window=[WindowSpec(order="D1", range="trailing 7 day")],
            )],
            materialization=MaterializationConfig(
                schedule="every 6 hours",
                materialized_views=[MaterializedViewDef(
                    name="baseline", type="unaggregated",
                )],
            ),
        )
        assert spec.comment == "Test metric view"
        assert spec.materialization is not None


class TestJoinDef:
    def test_valid_on_join(self):
        j = JoinDef(name="dim", source="cat.sch.dim", on="source.key = dim.key")
        assert j.on == "source.key = dim.key"

    def test_valid_using_join(self):
        j = JoinDef(name="dim", source="cat.sch.dim", using=["shared_key"])
        assert j.using == ["shared_key"]

    def test_rejects_both_on_and_using(self):
        with pytest.raises(ValueError, match="not both"):
            JoinDef(name="dim", source="cat.sch.dim", on="a = b", using=["c"])

    def test_rejects_neither_on_nor_using(self):
        with pytest.raises(ValueError, match="either"):
            JoinDef(name="dim", source="cat.sch.dim")

    def test_recursive_snowflake_joins(self):
        j = JoinDef(
            name="customer", source="cat.sch.customer",
            on="source.cust_key = customer.cust_key",
            joins=[JoinDef(
                name="nation", source="cat.sch.nation",
                on="customer.nation_key = nation.nation_key",
            )],
        )
        assert len(j.joins) == 1
        assert j.joins[0].name == "nation"


class TestDiscoveredTable:
    def test_fqn_property(self):
        t = DiscoveredTable(
            catalog="cat", schema_name="sch", table_name="tbl",
            columns=[DiscoveredColumn(name="col1", type_name="STRING")],
        )
        assert t.fqn == "cat.sch.tbl"


class TestDeployResult:
    def test_success_result(self):
        r = DeployResult(
            yaml_file="test.yaml", view_fqn="cat.sch.v",
            status="success", sql="CREATE ...",
        )
        assert r.error is None

    def test_failed_result(self):
        r = DeployResult(
            yaml_file="test.yaml", view_fqn="cat.sch.v",
            status="failed", sql="CREATE ...", error="boom",
        )
        assert r.error == "boom"
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
pytest tests/test_models.py -v
```

Expected: `ModuleNotFoundError: No module named 'metricviews.models'`

- [ ] **Step 3: Implement models.py**

File: `src/metricviews/models.py`

```python
"""Pydantic models for Databricks metric view YAML spec v1.1."""
from __future__ import annotations
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator
from enum import Enum


class ColumnRole(str, Enum):
    """Classification of a discovered column."""
    DIMENSION = "dimension"
    MEASURE = "measure"
    IGNORE = "ignore"


# ── YAML Spec Models ──────────────────────────────────────────────────

class WindowSpec(BaseModel):
    """Experimental — window specification for windowed/cumulative/semiadditive measures."""
    order: str
    range: str
    semiadditive: str | None = None


class DimensionDef(BaseModel):
    name: str
    expr: str
    comment: str | None = None
    display_name: str | None = None
    format: dict | None = None
    synonyms: list[str] | None = None


class MeasureDef(BaseModel):
    name: str
    expr: str
    comment: str | None = None
    display_name: str | None = None
    format: dict | None = None
    synonyms: list[str] | None = None
    window: list[WindowSpec] | None = None


class JoinDef(BaseModel):
    name: str
    source: str
    on: str | None = None
    using: list[str] | None = None
    joins: list[JoinDef] | None = None

    @model_validator(mode="after")
    def exactly_one_join_key(self) -> JoinDef:
        if self.on and self.using:
            raise ValueError(f"Join '{self.name}': specify 'on' or 'using', not both")
        if not self.on and not self.using:
            raise ValueError(f"Join '{self.name}': specify either 'on' or 'using'")
        return self

JoinDef.model_rebuild()


class MaterializedViewDef(BaseModel):
    name: str
    type: str
    dimensions: list[str] | None = None
    measures: list[str] | None = None


class MaterializationConfig(BaseModel):
    schedule: str
    mode: str = "relaxed"
    materialized_views: list[MaterializedViewDef]


class MetricViewSpec(BaseModel):
    """1:1 mapping to Databricks metric view YAML spec v1.1."""
    model_config = ConfigDict(extra="forbid")

    version: str = "1.1"
    source: str
    comment: str | None = None
    filter: str | None = None
    joins: list[JoinDef] | None = None
    dimensions: list[DimensionDef] = Field(min_length=1)
    measures: list[MeasureDef] = Field(min_length=1)
    materialization: MaterializationConfig | None = None

    @model_validator(mode="after")
    def no_duplicate_names(self) -> MetricViewSpec:
        names = [d.name for d in self.dimensions] + [m.name for m in self.measures]
        dupes = [n for n in names if names.count(n) > 1]
        if dupes:
            raise ValueError(f"Duplicate column names: {set(dupes)}")
        return self


# ── Discovery Models ──────────────────────────────────────────────────

class DiscoveredColumn(BaseModel):
    """A column discovered from Unity Catalog introspection."""
    name: str
    type_name: str
    comment: str | None = None
    role: ColumnRole | None = None


class DiscoveredTable(BaseModel):
    """A table discovered from Unity Catalog."""
    catalog: str
    schema_name: str
    table_name: str
    columns: list[DiscoveredColumn]
    comment: str | None = None

    @property
    def fqn(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.table_name}"


class DeployResult(BaseModel):
    """Result of deploying a single metric view."""
    yaml_file: str
    view_fqn: str
    status: Literal["success", "failed", "dry_run"]
    sql: str
    error: str | None = None
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
pytest tests/test_models.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/metricviews/models.py tests/test_models.py
git commit -m "feat: add Pydantic domain models"
```

---

## Task 3: Column Classification Heuristics

**Files:**
- Create: `src/metricviews/heuristics.py`
- Create: `tests/test_heuristics.py`

- [ ] **Step 1: Write failing tests**

File: `tests/test_heuristics.py`

```python
"""Tests for column classification heuristics."""
import pytest
from metricviews.models import DiscoveredColumn, ColumnRole
from metricviews.heuristics import classify_column, suggest_aggregation, classify_table


class TestClassifyColumn:
    """Priority: name pattern > type inference > default DIMENSION."""

    @pytest.mark.parametrize("name,type_name,expected", [
        # IGNORE — name patterns
        ("_fivetran_synced", "TIMESTAMP", ColumnRole.IGNORE),
        ("etl_batch_id", "STRING", ColumnRole.IGNORE),
        ("record_created_at", "TIMESTAMP", ColumnRole.IGNORE),
        # DIMENSION — name patterns (name wins over type)
        ("customer_id", "BIGINT", ColumnRole.DIMENSION),   # _id overrides numeric type
        ("order_key", "BIGINT", ColumnRole.DIMENSION),
        ("order_date", "DATE", ColumnRole.DIMENSION),
        ("is_active", "BOOLEAN", ColumnRole.DIMENSION),
        ("customer_name", "STRING", ColumnRole.DIMENSION),
        # MEASURE — name patterns
        ("total_amount", "DECIMAL(10,2)", ColumnRole.MEASURE),
        ("order_count", "BIGINT", ColumnRole.MEASURE),
        ("unit_price", "DOUBLE", ColumnRole.MEASURE),
        # Type fallback (no name match)
        ("latitude", "DOUBLE", ColumnRole.MEASURE),
        ("description", "STRING", ColumnRole.DIMENSION),
        ("payload", "STRUCT", ColumnRole.DIMENSION),   # unknown type → default DIMENSION
    ])
    def test_classify_column(self, name, type_name, expected):
        col = DiscoveredColumn(name=name, type_name=type_name)
        assert classify_column(col) == expected


class TestSuggestAggregation:
    @pytest.mark.parametrize("col_name,col_type,expected", [
        ("order_count", "BIGINT", "SUM(order_count)"),
        ("conversion_rate", "DOUBLE", "AVG(conversion_rate)"),
        ("total_amount", "DECIMAL", "SUM(total_amount)"),
        ("max_price", "DOUBLE", "MAX(max_price)"),
        ("min_price", "DOUBLE", "MIN(min_price)"),
    ])
    def test_suggest_aggregation(self, col_name, col_type, expected):
        assert suggest_aggregation(col_name, col_type) == expected


class TestClassifyTable:
    def test_returns_copies_with_roles_assigned(self):
        columns = [
            DiscoveredColumn(name="order_id", type_name="BIGINT"),
            DiscoveredColumn(name="total_amount", type_name="DECIMAL(10,2)"),
            DiscoveredColumn(name="_etl_loaded", type_name="TIMESTAMP"),
        ]
        result = classify_table(columns)
        assert len(result) == 3
        assert result[0].role == ColumnRole.DIMENSION
        assert result[1].role == ColumnRole.MEASURE
        assert result[2].role == ColumnRole.IGNORE
        # Verify originals are not modified
        assert columns[0].role is None
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
pytest tests/test_heuristics.py -v
```

Expected: `ModuleNotFoundError: No module named 'metricviews.heuristics'`

- [ ] **Step 3: Implement heuristics.py**

File: `src/metricviews/heuristics.py`

```python
"""Heuristics for classifying columns as dimension, measure, or ignore.

Priority order (highest wins):
  1. Name pattern match
  2. Type-based inference
  3. Conservative default → DIMENSION
"""
from __future__ import annotations
import re
from .models import ColumnRole, DiscoveredColumn

# ── NAME PATTERNS ──────────────────────────────────────────────────

_IGNORE_PATTERNS: list[str] = [
    r"^_.*",
    r".*_(created|updated|modified|loaded|ingested)_at$",
    r"^etl_.*", r"^dwh_.*", r"^meta_.*",
    r"^__.*",
]

_DIMENSION_NAME_PATTERNS: list[str] = [
    r".*_id$", r".*_key$", r".*_sk$",
    r".*_code$", r".*_name$", r".*_desc$",
    r".*_type$", r".*_status$", r".*_state$",
    r".*_category$", r".*_class$", r".*_group$",
    r".*_region$", r".*_country$", r".*_city$",
    r"^is_.*", r"^has_.*", r"^flag_.*",
    r".*_date$", r".*_month$", r".*_year$",
    r".*_at$",
]

_MEASURE_NAME_PATTERNS: list[str] = [
    r".*_amount$", r".*_amt$",
    r".*_total$", r".*_sum$",
    r".*_count$", r".*_cnt$",
    r".*_qty$", r".*_quantity$",
    r".*_price$", r".*_cost$", r".*_revenue$",
    r".*_rate$", r".*_ratio$", r".*_pct$", r".*_percent$",
    r".*_balance$", r".*_fee$", r".*_tax$",
    r".*_weight$", r".*_volume$",
    r".*_score$", r".*_value$",
]

# ── TYPE SETS ──────────────────────────────────────────────────────

# SDK ColumnTypeName enum values + common SQL aliases for compatibility
_DIMENSION_TYPES: set[str] = {
    "STRING", "VARCHAR", "CHAR", "TEXT",        # SDK: STRING, CHAR
    "DATE", "TIMESTAMP", "TIMESTAMP_NTZ",       # SDK: DATE, TIMESTAMP, TIMESTAMP_NTZ
    "BOOLEAN", "BINARY",                        # SDK: BOOLEAN, BINARY
}

_MEASURE_TYPES: set[str] = {
    "INT", "INTEGER",                           # SDK: INT
    "LONG", "BIGINT",                           # SDK: LONG
    "SHORT", "SMALLINT",                        # SDK: SHORT
    "BYTE", "TINYINT",                          # SDK: BYTE
    "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC",    # SDK: FLOAT, DOUBLE, DECIMAL
}


def _matches_any(name: str, patterns: list[str]) -> bool:
    return any(re.match(p, name) for p in patterns)


def _base_type(type_name: str) -> str:
    """'DECIMAL(10,2)' → 'DECIMAL'"""
    return type_name.upper().split("(")[0].strip()


def classify_column(col: DiscoveredColumn) -> ColumnRole:
    """Classify a single column. Deterministic, stateless, testable."""
    name = col.name.lower()

    if _matches_any(name, _IGNORE_PATTERNS):
        return ColumnRole.IGNORE
    if _matches_any(name, _DIMENSION_NAME_PATTERNS):
        return ColumnRole.DIMENSION
    if _matches_any(name, _MEASURE_NAME_PATTERNS):
        return ColumnRole.MEASURE

    base = _base_type(col.type_name)
    if base in _DIMENSION_TYPES:
        return ColumnRole.DIMENSION
    if base in _MEASURE_TYPES:
        return ColumnRole.MEASURE

    return ColumnRole.DIMENSION


def suggest_aggregation(col_name: str, col_type: str) -> str:
    """Suggest a default aggregation function for a measure column."""
    name = col_name.lower()
    if any(kw in name for kw in ("count", "cnt")):
        return f"SUM({col_name})"
    if any(kw in name for kw in ("avg", "rate", "ratio", "pct", "percent")):
        return f"AVG({col_name})"
    if "max" in name:
        return f"MAX({col_name})"
    if "min" in name:
        return f"MIN({col_name})"
    return f"SUM({col_name})"


def classify_table(columns: list[DiscoveredColumn]) -> list[DiscoveredColumn]:
    """Classify all columns. Returns copies with role assigned."""
    classified = []
    for col in columns:
        col_copy = col.model_copy()
        col_copy.role = classify_column(col)
        classified.append(col_copy)
    return classified
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
pytest tests/test_heuristics.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/metricviews/heuristics.py tests/test_heuristics.py
git commit -m "feat: add column classification heuristics"
```

---

## Task 4: Unity Catalog Introspector

**Files:**
- Create: `src/metricviews/introspector.py`
- Create: `tests/test_introspector.py`

- [ ] **Step 1: Write failing tests**

File: `tests/test_introspector.py`

```python
"""Tests for Unity Catalog introspector — all SDK calls mocked."""
from unittest.mock import MagicMock, patch
import pytest
from metricviews.introspector import create_client, list_tables, discover_table


class TestCreateClient:
    @patch("metricviews.introspector.WorkspaceClient")
    def test_creates_client_with_explicit_args(self, mock_ws):
        create_client(host="https://test.cloud.databricks.com", token="dapi123")
        mock_ws.assert_called_once_with(
            host="https://test.cloud.databricks.com", token="dapi123",
        )

    @patch("metricviews.introspector.WorkspaceClient")
    def test_creates_client_with_defaults(self, mock_ws):
        create_client()
        mock_ws.assert_called_once_with()


class TestListTables:
    def _make_table(self, name, table_type="MANAGED"):
        t = MagicMock()
        t.name = name
        t.table_type = MagicMock()
        t.table_type.value = table_type
        return t

    def test_lists_tables_sorted(self):
        client = MagicMock()
        client.tables.list.return_value = [
            self._make_table("fct_orders"),
            self._make_table("dim_customer"),
        ]
        result = list_tables(client, "cat", "sch")
        assert result == ["dim_customer", "fct_orders"]

    def test_excludes_views_by_default(self):
        client = MagicMock()
        client.tables.list.return_value = [
            self._make_table("fct_orders"),
            self._make_table("v_summary", "VIEW"),
        ]
        result = list_tables(client, "cat", "sch")
        assert result == ["fct_orders"]

    def test_excludes_metric_views_and_materialized_views(self):
        client = MagicMock()
        client.tables.list.return_value = [
            self._make_table("fct_orders"),
            self._make_table("mv_summary", "MATERIALIZED_VIEW"),
            self._make_table("metric_kpis", "METRIC_VIEW"),
            self._make_table("stream_events", "STREAMING_TABLE"),
        ]
        result = list_tables(client, "cat", "sch")
        assert result == ["fct_orders"]

    def test_includes_views_when_requested(self):
        client = MagicMock()
        client.tables.list.return_value = [
            self._make_table("fct_orders"),
            self._make_table("v_summary", "VIEW"),
        ]
        result = list_tables(client, "cat", "sch", include_views=True)
        assert result == ["fct_orders", "v_summary"]

    def test_applies_glob_filter(self):
        client = MagicMock()
        client.tables.list.return_value = [
            self._make_table("fct_orders"),
            self._make_table("dim_customer"),
        ]
        result = list_tables(client, "cat", "sch", table_filter="fct_*")
        assert result == ["fct_orders"]


class TestDiscoverTable:
    def test_returns_discovered_table_with_columns(self):
        col1 = MagicMock()
        col1.name = "order_id"
        col1.type_name = MagicMock(value="BIGINT")
        col1.comment = "Primary key"

        col2 = MagicMock()
        col2.name = "amount"
        col2.type_name = MagicMock(value="DECIMAL(10,2)")
        col2.comment = None

        table_info = MagicMock()
        table_info.columns = [col1, col2]
        table_info.comment = "Orders table"

        client = MagicMock()
        client.tables.get.return_value = table_info

        result = discover_table(client, "cat", "sch", "fct_orders")
        assert result.fqn == "cat.sch.fct_orders"
        assert len(result.columns) == 2
        assert result.columns[0].name == "order_id"
        assert result.columns[1].type_name == "DECIMAL(10,2)"
        assert result.comment == "Orders table"

    def test_handles_table_with_no_columns(self):
        table_info = MagicMock()
        table_info.columns = None
        table_info.comment = None

        client = MagicMock()
        client.tables.get.return_value = table_info

        result = discover_table(client, "cat", "sch", "empty_table")
        assert result.columns == []
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
pytest tests/test_introspector.py -v
```

Expected: `ModuleNotFoundError: No module named 'metricviews.introspector'`

- [ ] **Step 3: Implement introspector.py**

File: `src/metricviews/introspector.py`

```python
"""Unity Catalog introspection — discover tables and their schemas."""
from __future__ import annotations

from databricks.sdk import WorkspaceClient
from .models import DiscoveredTable, DiscoveredColumn
import fnmatch
import logging

logger = logging.getLogger(__name__)


def create_client(
    host: str | None = None,
    token: str | None = None,
) -> WorkspaceClient:
    """Create a Databricks SDK client.

    Falls back to DATABRICKS_HOST / DATABRICKS_TOKEN env vars
    and ~/.databrickscfg profile if not provided.
    """
    kwargs = {}
    if host:
        kwargs["host"] = host
    if token:
        kwargs["token"] = token
    return WorkspaceClient(**kwargs)


_VIEW_TYPES = {"VIEW", "MATERIALIZED_VIEW", "METRIC_VIEW", "STREAMING_TABLE"}


def list_tables(
    client: WorkspaceClient,
    catalog: str,
    schema: str,
    table_filter: str | None = None,
    include_views: bool = False,
) -> list[str]:
    """List table names in a schema. Optional glob filter (e.g. 'fct_*')."""
    tables = client.tables.list(catalog_name=catalog, schema_name=schema)
    names = []
    for t in tables:
        if not include_views and t.table_type and t.table_type.value in _VIEW_TYPES:
            continue
        if table_filter and not fnmatch.fnmatch(t.name, table_filter):
            continue
        names.append(t.name)
    return sorted(names)


def discover_table(
    client: WorkspaceClient,
    catalog: str,
    schema: str,
    table_name: str,
) -> DiscoveredTable:
    """Get full column metadata for a single table."""
    fqn = f"{catalog}.{schema}.{table_name}"
    table_info = client.tables.get(full_name=fqn)

    columns = []
    for col in (table_info.columns or []):
        columns.append(DiscoveredColumn(
            name=col.name,
            # col.type_name is a ColumnTypeName enum — extract .value for the string
            type_name=col.type_name.value if col.type_name else "STRING",
            comment=col.comment,
        ))

    return DiscoveredTable(
        catalog=catalog,
        schema_name=schema,
        table_name=table_name,
        columns=columns,
        comment=table_info.comment,
    )
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
pytest tests/test_introspector.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/metricviews/introspector.py tests/test_introspector.py
git commit -m "feat: add Unity Catalog introspector"
```

---

## Task 5: YAML Generator

**Files:**
- Create: `src/metricviews/generator.py`
- Create: `tests/test_generator.py`

- [ ] **Step 1: Write failing tests**

File: `tests/test_generator.py`

```python
"""Tests for metric view YAML generation."""
import pytest
from pathlib import Path
from metricviews.models import (
    DiscoveredTable, DiscoveredColumn, MetricViewSpec,
    DimensionDef, MeasureDef,
)
from metricviews.generator import spec_from_tables, spec_to_yaml, write_yaml_file


def _make_source():
    return DiscoveredTable(
        catalog="cat", schema_name="sch", table_name="fct_orders",
        columns=[
            DiscoveredColumn(name="order_id", type_name="BIGINT"),
            DiscoveredColumn(name="order_date", type_name="DATE"),
            DiscoveredColumn(name="total_amount", type_name="DECIMAL(10,2)"),
            DiscoveredColumn(name="_etl_loaded_at", type_name="TIMESTAMP"),
        ],
    )


def _make_dim():
    return DiscoveredTable(
        catalog="cat", schema_name="sch", table_name="dim_customer",
        columns=[
            DiscoveredColumn(name="customer_id", type_name="BIGINT"),
            DiscoveredColumn(name="customer_name", type_name="STRING"),
        ],
    )


class TestSpecFromTables:
    def test_row_count_is_always_first_measure(self):
        spec = spec_from_tables(_make_source())
        assert spec.measures[0].name == "Row Count"
        assert spec.measures[0].expr == "COUNT(1)"

    def test_classifies_source_columns(self):
        spec = spec_from_tables(_make_source())
        dim_names = [d.name for d in spec.dimensions]
        measure_names = [m.name for m in spec.measures]
        assert "Order Id" in dim_names
        assert "Order Date" in dim_names
        assert "Total Amount" in measure_names
        # _etl_loaded_at should be ignored (not in dims or measures)
        all_names = dim_names + measure_names
        assert not any("Etl" in n for n in all_names)

    def test_join_auto_detection_shared_key(self):
        source = DiscoveredTable(
            catalog="cat", schema_name="sch", table_name="fct",
            columns=[
                DiscoveredColumn(name="customer_id", type_name="BIGINT"),
                DiscoveredColumn(name="amount", type_name="DECIMAL"),
            ],
        )
        dim = DiscoveredTable(
            catalog="cat", schema_name="sch", table_name="dim_customer",
            columns=[
                DiscoveredColumn(name="customer_id", type_name="BIGINT"),
                DiscoveredColumn(name="customer_name", type_name="STRING"),
            ],
        )
        spec = spec_from_tables(source, [dim])
        assert spec.joins is not None
        assert spec.joins[0].using == ["customer_id"]

    def test_join_placeholder_when_no_key_found(self):
        source = DiscoveredTable(
            catalog="cat", schema_name="sch", table_name="fct",
            columns=[DiscoveredColumn(name="amount", type_name="DECIMAL")],
        )
        dim = DiscoveredTable(
            catalog="cat", schema_name="sch", table_name="dim_other",
            columns=[DiscoveredColumn(name="name", type_name="STRING")],
        )
        spec = spec_from_tables(source, [dim])
        assert "???" in spec.joins[0].on

    def test_dim_table_columns_become_dimensions(self):
        spec = spec_from_tables(_make_source(), [_make_dim()])
        dim_names = [d.name for d in spec.dimensions]
        assert "Customer Name" in dim_names
        # customer_id is a key column — should NOT appear as a dimension from the dim table
        assert dim_names.count("Customer Id") == 1  # only from source, not from dim


class TestSpecToYaml:
    def test_on_key_is_always_quoted(self):
        spec = MetricViewSpec(
            source="cat.sch.tbl",
            joins=[{"name": "dim", "source": "cat.sch.dim", "on": "source.k = dim.k"}],
            dimensions=[DimensionDef(name="D1", expr="col1")],
            measures=[MeasureDef(name="M1", expr="SUM(col2)")],
        )
        yaml_str = spec_to_yaml(spec)
        assert '"on":' in yaml_str
        assert "\n  on:" not in yaml_str  # bare on: must never appear

    def test_output_contains_version(self):
        spec = MetricViewSpec(
            source="cat.sch.tbl",
            dimensions=[DimensionDef(name="D1", expr="col1")],
            measures=[MeasureDef(name="M1", expr="SUM(col2)")],
        )
        yaml_str = spec_to_yaml(spec)
        assert yaml_str.startswith('version: "1.1"')

    def test_snowflake_join_nesting_indentation(self):
        """Nested joins (snowflake schema) must have correct indentation."""
        from metricviews.models import JoinDef
        spec = MetricViewSpec(
            source="cat.sch.tbl",
            joins=[JoinDef(
                name="customer", source="cat.sch.customer",
                on="source.cust_key = customer.cust_key",
                joins=[JoinDef(
                    name="nation", source="cat.sch.nation",
                    on="customer.nation_key = nation.nation_key",
                )],
            )],
            dimensions=[DimensionDef(name="D1", expr="col1")],
            measures=[MeasureDef(name="M1", expr="SUM(col2)")],
        )
        yaml_str = spec_to_yaml(spec)
        # Nested join should be indented further than parent
        assert "      - name: nation" in yaml_str
        assert '"on": customer.nation_key' in yaml_str


class TestWriteYamlFile:
    def test_creates_file(self, tmp_path):
        spec = MetricViewSpec(
            source="cat.sch.tbl",
            dimensions=[DimensionDef(name="D1", expr="col1")],
            measures=[MeasureDef(name="M1", expr="SUM(col2)")],
        )
        out = tmp_path / "test.yaml"
        result = write_yaml_file(spec, out)
        assert result == out
        assert out.exists()
        assert 'version: "1.1"' in out.read_text()

    def test_skips_existing_file_without_overwrite(self, tmp_path):
        spec = MetricViewSpec(
            source="cat.sch.tbl",
            dimensions=[DimensionDef(name="D1", expr="col1")],
            measures=[MeasureDef(name="M1", expr="SUM(col2)")],
        )
        out = tmp_path / "test.yaml"
        out.write_text("existing")
        result = write_yaml_file(spec, out, overwrite=False)
        assert result is None
        assert out.read_text() == "existing"

    def test_overwrites_when_flag_set(self, tmp_path):
        spec = MetricViewSpec(
            source="cat.sch.tbl",
            dimensions=[DimensionDef(name="D1", expr="col1")],
            measures=[MeasureDef(name="M1", expr="SUM(col2)")],
        )
        out = tmp_path / "test.yaml"
        out.write_text("existing")
        result = write_yaml_file(spec, out, overwrite=True)
        assert result == out
        assert "existing" not in out.read_text()

    def test_creates_parent_dirs(self, tmp_path):
        spec = MetricViewSpec(
            source="cat.sch.tbl",
            dimensions=[DimensionDef(name="D1", expr="col1")],
            measures=[MeasureDef(name="M1", expr="SUM(col2)")],
        )
        out = tmp_path / "deep" / "nested" / "test.yaml"
        result = write_yaml_file(spec, out)
        assert result == out
        assert out.exists()
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
pytest tests/test_generator.py -v
```

Expected: `ModuleNotFoundError: No module named 'metricviews.generator'`

- [ ] **Step 3: Implement generator.py**

File: `src/metricviews/generator.py`

```python
"""Generate metric view YAML files from discovered table metadata."""
from __future__ import annotations

from pathlib import Path
from typing import Any
from .models import (
    MetricViewSpec, DimensionDef, MeasureDef, JoinDef,
    DiscoveredTable, ColumnRole,
)
from .heuristics import classify_table, suggest_aggregation
import logging

logger = logging.getLogger(__name__)


def _humanize(col_name: str) -> str:
    """'total_order_amount' → 'Total Order Amount'"""
    return col_name.replace("_", " ").title()


def _find_join_key(source: DiscoveredTable, dim: DiscoveredTable) -> str | None:
    """Auto-detect join key between source and dimension table."""
    source_cols = {c.name.lower() for c in source.columns}
    dim_cols = {c.name.lower() for c in dim.columns}

    key_suffixes = ("_key", "_id", "_sk")
    source_keys = {c for c in source_cols if any(c.endswith(s) for s in key_suffixes)}
    dim_keys = {c for c in dim_cols if any(c.endswith(s) for s in key_suffixes)}
    shared = source_keys & dim_keys
    if shared:
        return sorted(shared)[0]

    dim_name = dim.table_name.lower().removeprefix("dim_")
    for suffix in key_suffixes:
        candidate = f"{dim_name}{suffix}"
        if candidate in source_cols:
            for dc in dim_cols:
                if dc.endswith(suffix):
                    return f"source.{candidate} = {dim.table_name}.{dc}"
    return None


def spec_from_tables(
    source: DiscoveredTable,
    dim_tables: list[DiscoveredTable] | None = None,
) -> MetricViewSpec:
    """Generate a MetricViewSpec from a fact table + optional dimension tables."""
    classified_source = classify_table(source.columns)

    dimensions: list[DimensionDef] = []
    measures: list[MeasureDef] = []
    joins: list[JoinDef] = []

    for col in classified_source:
        if col.role == ColumnRole.DIMENSION:
            dimensions.append(DimensionDef(
                name=_humanize(col.name), expr=col.name, comment=col.comment,
            ))
        elif col.role == ColumnRole.MEASURE:
            measures.append(MeasureDef(
                name=_humanize(col.name),
                expr=suggest_aggregation(col.name, col.type_name),
                comment=col.comment,
            ))

    measures.insert(0, MeasureDef(
        name="Row Count", expr="COUNT(1)", comment="Total number of records",
    ))

    for dim in (dim_tables or []):
        join_key = _find_join_key(source, dim)

        join_def_kwargs: dict[str, Any] = {"name": dim.table_name, "source": dim.fqn}
        if join_key and "=" in join_key:
            join_def_kwargs["on"] = join_key
        elif join_key:
            join_def_kwargs["using"] = [join_key]
        else:
            join_def_kwargs["on"] = f"source.??? = {dim.table_name}.???"

        joins.append(JoinDef(**join_def_kwargs))

        classified_dim = classify_table(dim.columns)
        for col in classified_dim:
            if col.role == ColumnRole.IGNORE:
                continue
            if col.name.lower().endswith(("_key", "_id", "_sk")):
                continue
            dimensions.append(DimensionDef(
                name=_humanize(col.name),
                expr=f"{dim.table_name}.{col.name}",
                comment=col.comment,
            ))

    return MetricViewSpec(
        source=source.fqn,
        comment=source.comment or f"Metric view for {source.table_name}",
        joins=joins if joins else None,
        dimensions=dimensions,
        measures=measures,
    )


def spec_to_yaml(spec: MetricViewSpec) -> str:
    """Serialize MetricViewSpec to YAML string. Manual formatting — not yaml.dump().

    NOTE: Does not render materialization or window specs. These are hand-authored
    by the user directly in YAML — they are never generated by spec_from_tables().
    """
    lines = [f'version: "{spec.version}"']

    if spec.comment:
        lines.append(f'comment: "{spec.comment}"')

    lines.append(f"source: {spec.source}")

    if spec.filter:
        lines.append(f'filter: "{spec.filter}"')

    if spec.joins:
        lines.append("joins:")
        for j in spec.joins:
            _render_join(lines, j, indent=2)

    lines.append("dimensions:")
    for d in spec.dimensions:
        lines.append(f"  - name: {d.name}")
        lines.append(f'    expr: "{d.expr}"')
        if d.comment:
            lines.append(f'    comment: "{d.comment}"')

    lines.append("measures:")
    for m in spec.measures:
        lines.append(f"  - name: {m.name}")
        lines.append(f'    expr: "{m.expr}"')
        if m.comment:
            lines.append(f'    comment: "{m.comment}"')

    return "\n".join(lines) + "\n"


def _render_join(lines: list[str], join: JoinDef, indent: int) -> None:
    """Render a single join (and nested children) to YAML lines."""
    pad = " " * indent
    lines.append(f"{pad}- name: {join.name}")
    lines.append(f"{pad}  source: {join.source}")
    if join.on:
        lines.append(f'{pad}  "on": {join.on}')
    if join.using:
        lines.append(f"{pad}  using:")
        for u in join.using:
            lines.append(f"{pad}    - {u}")
    if join.joins:
        lines.append(f"{pad}  joins:")
        for child in join.joins:
            _render_join(lines, child, indent + 4)


def write_yaml_file(
    spec: MetricViewSpec,
    output_path: str | Path,
    overwrite: bool = False,
) -> Path | None:
    """Write YAML file. Returns path if written, None if skipped."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists() and not overwrite:
        logger.info(f"Skipping {path} (already exists, use --overwrite)")
        return None

    yaml_content = spec_to_yaml(spec)
    path.write_text(yaml_content)
    logger.info(f"Generated {path}")
    return path
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
pytest tests/test_generator.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/metricviews/generator.py tests/test_generator.py
git commit -m "feat: add metric view YAML generator"
```

---

## Task 6: YAML Validator

**Files:**
- Create: `src/metricviews/validator.py`
- Create: `tests/test_validator.py`
- Create: `tests/fixtures/*.yaml` (6 fixture files)

**Critical fix:** YAML `on` boolean pre-check runs BEFORE Pydantic parsing. When `yaml.safe_load()` sees unquoted `on:`, it becomes `{True: "value"}` in the dict. The validator detects boolean keys in join dicts, rewrites them to `"on"`, and emits a warning — so users get a clear message instead of a cryptic Pydantic error.

- [ ] **Step 1: Create test fixtures**

File: `tests/fixtures/sample_orders.yaml`

```yaml
version: "1.1"
comment: "Orders KPIs"
source: analytics.gold.fct_orders
dimensions:
  - name: Order Date
    expr: "o_orderdate"
  - name: Customer Id
    expr: "o_custkey"
measures:
  - name: Row Count
    expr: "COUNT(1)"
    comment: "Total number of records"
  - name: Total Revenue
    expr: "SUM(o_totalprice)"
```

File: `tests/fixtures/star_schema_with_joins.yaml`

```yaml
version: "1.1"
source: analytics.gold.fct_orders
joins:
  - name: dim_customer
    source: analytics.gold.dim_customer
    "on": source.o_custkey = dim_customer.c_custkey
dimensions:
  - name: Order Date
    expr: "o_orderdate"
  - name: Customer Name
    expr: "dim_customer.c_name"
measures:
  - name: Row Count
    expr: "COUNT(1)"
  - name: Total Revenue
    expr: "SUM(o_totalprice)"
```

File: `tests/fixtures/with_window_measures.yaml`

```yaml
version: "1.1"
source: analytics.gold.fct_orders
dimensions:
  - name: Order Date
    expr: "o_orderdate"
measures:
  - name: Row Count
    expr: "COUNT(1)"
  - name: Trailing 7 Day Revenue
    expr: "SUM(o_totalprice)"
    window:
      - order: Order Date
        range: trailing 7 day
```

File: `tests/fixtures/with_materialization.yaml`

```yaml
version: "1.1"
source: analytics.gold.fct_orders
dimensions:
  - name: Order Date
    expr: "o_orderdate"
measures:
  - name: Row Count
    expr: "COUNT(1)"
materialization:
  schedule: every 6 hours
  mode: relaxed
  materialized_views:
    - name: baseline
      type: unaggregated
```

File: `tests/fixtures/invalid_missing_measures.yaml`

```yaml
version: "1.1"
source: analytics.gold.fct_orders
dimensions:
  - name: Order Date
    expr: "o_orderdate"
```

File: `tests/fixtures/unquoted_on_key.yaml`

```yaml
version: "1.1"
source: analytics.gold.fct_orders
joins:
  - name: dim_customer
    source: analytics.gold.dim_customer
    on: source.o_custkey = dim_customer.c_custkey
dimensions:
  - name: Order Date
    expr: "o_orderdate"
measures:
  - name: Row Count
    expr: "COUNT(1)"
```

- [ ] **Step 2: Write failing tests**

File: `tests/test_validator.py`

```python
"""Tests for YAML metric view validator."""
import pytest
from pathlib import Path
from metricviews.validator import validate_file, validate_directory, YamlValidationError

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
        assert any("on" in e.message.lower() and "quoted" in e.message.lower()
                    for e in warnings)
        # Must NOT have a Pydantic schema error about missing on/using
        schema_errors = [e for e in errors if "specify either" in e.message.lower()]
        assert len(schema_errors) == 0

    def test_placeholder_join_key_is_error(self, tmp_path):
        yaml_content = '''version: "1.1"
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
'''
        f = tmp_path / "placeholder.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        assert any("???" in e.message for e in errors)

    def test_too_many_synonyms_is_error(self, tmp_path):
        yaml_content = '''version: "1.1"
source: cat.sch.tbl
dimensions:
  - name: D1
    expr: "col1"
    synonyms: ["a","b","c","d","e","f","g","h","i","j","k"]
measures:
  - name: M1
    expr: "SUM(col2)"
'''
        f = tmp_path / "synonyms.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        assert any("synonym" in e.message.lower() for e in errors)

    def test_unknown_format_type_is_error(self, tmp_path):
        yaml_content = '''version: "1.1"
source: cat.sch.tbl
dimensions:
  - name: D1
    expr: "col1"
    format:
      type: "invalid_type"
measures:
  - name: M1
    expr: "SUM(col2)"
'''
        f = tmp_path / "format.yaml"
        f.write_text(yaml_content)
        errors = validate_file(f)
        assert any("format type" in e.message.lower() for e in errors)

    def test_non_fqn_source_emits_warning(self, tmp_path):
        yaml_content = '''version: "1.1"
source: not_a_valid_source
dimensions:
  - name: D1
    expr: "col1"
measures:
  - name: M1
    expr: "SUM(col2)"
'''
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
```

- [ ] **Step 3: Run tests — verify they fail**

```bash
pytest tests/test_validator.py -v
```

Expected: `ModuleNotFoundError: No module named 'metricviews.validator'`

- [ ] **Step 4: Implement validator.py**

File: `src/metricviews/validator.py`

```python
"""Validate metric view YAML files before deployment."""
from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass
from typing import Literal
from .models import JoinDef, MetricViewSpec
import pydantic
import yaml
import re
import logging

logger = logging.getLogger(__name__)


@dataclass
class YamlValidationError:
    """A validation finding. Named to avoid collision with pydantic.ValidationError."""
    file: str
    message: str
    severity: Literal["error", "warning"] = "error"


_AGG_FUNCTIONS = (
    "SUM(", "COUNT(", "AVG(", "MIN(", "MAX(",
    "COUNT_IF(", "APPROX_COUNT_DISTINCT(",
    "COLLECT_SET(", "COLLECT_LIST(",
    "PERCENTILE(", "STDDEV(",
)

_FQN_PATTERN = re.compile(r"^[\w]+\.[\w]+\.[\w]+$")

_VALID_FORMAT_TYPES = {"number", "currency", "percentage", "byte", "date", "date_time"}


def _fix_yaml_on_boolean_keys(raw: dict) -> list[YamlValidationError]:
    """Pre-Pydantic fix: yaml.safe_load parses unquoted 'on:' as boolean True.

    Detects {True: "join condition"} in join dicts and rewrites to {"on": "value"}.
    Returns warnings for each rewrite.
    """
    warnings: list[YamlValidationError] = []

    def _fix_joins(joins_list: list) -> None:
        for join_dict in joins_list:
            if not isinstance(join_dict, dict):
                continue
            if True in join_dict:
                join_dict["on"] = join_dict.pop(True)
                join_name = join_dict.get("name", "unknown")
                warnings.append(YamlValidationError(
                    file="",  # caller fills in
                    message=(
                        f"Join '{join_name}': bare 'on:' was parsed as boolean True "
                        f"by YAML 1.1 — quote it as '\"on\":' in your YAML file"
                    ),
                    severity="warning",
                ))
            if "joins" in join_dict and isinstance(join_dict["joins"], list):
                _fix_joins(join_dict["joins"])

    if "joins" in raw and isinstance(raw["joins"], list):
        _fix_joins(raw["joins"])

    return warnings


def validate_file(yaml_path: str | Path) -> list[YamlValidationError]:
    """Validate a single YAML file. Returns empty list if valid."""
    path = Path(yaml_path)
    errors: list[YamlValidationError] = []
    name = path.name

    if not path.exists():
        return [YamlValidationError(name, f"File not found: {path}")]

    # 1. Valid YAML syntax
    try:
        raw = yaml.safe_load(path.read_text())
    except yaml.YAMLError as e:
        return [YamlValidationError(name, f"Invalid YAML: {e}")]

    if not isinstance(raw, dict):
        return [YamlValidationError(name, "YAML must be a mapping (dict)")]

    # 2. Fix YAML 'on' boolean keys BEFORE Pydantic parsing
    on_warnings = _fix_yaml_on_boolean_keys(raw)
    for w in on_warnings:
        w.file = name
    errors.extend(on_warnings)

    # 3. Pydantic structural validation
    try:
        spec = MetricViewSpec(**raw)
    except pydantic.ValidationError as e:
        return errors + [YamlValidationError(name, f"Schema validation failed: {e}")]

    # 4. Source should be FQN or SQL query
    if not _FQN_PATTERN.match(spec.source) and "SELECT" not in spec.source.upper():
        errors.append(YamlValidationError(
            name,
            f"Source '{spec.source}' should be fully-qualified (catalog.schema.table) or a SQL query",
            "warning",
        ))

    # 5. Measures should contain aggregate functions
    for m in spec.measures:
        expr_upper = m.expr.upper()
        has_agg = any(fn in expr_upper for fn in _AGG_FUNCTIONS)
        has_measure_ref = "MEASURE(" in expr_upper
        if not has_agg and not has_measure_ref and "/" not in m.expr:
            errors.append(YamlValidationError(
                name,
                f"Measure '{m.name}' may be missing aggregate function: {m.expr}",
                "warning",
            ))

    # 6. Experimental: materialization
    if spec.materialization:
        errors.append(YamlValidationError(
            name,
            "materialization is an Experimental feature — behavior may change",
            "warning",
        ))

    # 7. Experimental: window measures
    for m in (spec.measures or []):
        if m.window:
            errors.append(YamlValidationError(
                name,
                f"Measure '{m.name}' uses window (Experimental feature)",
                "warning",
            ))
            break

    # 8. Format type validation
    for col in list(spec.dimensions) + list(spec.measures):
        if col.format and isinstance(col.format, dict):
            fmt_type = col.format.get("type")
            if fmt_type and fmt_type not in _VALID_FORMAT_TYPES:
                errors.append(YamlValidationError(
                    name,
                    f"Column '{col.name}' has unknown format type '{fmt_type}'. "
                    f"Valid: {', '.join(sorted(_VALID_FORMAT_TYPES))}",
                ))

    # 9. Synonym count (max 10 per column)
    for col in list(spec.dimensions) + list(spec.measures):
        if col.synonyms and len(col.synonyms) > 10:
            errors.append(YamlValidationError(
                name,
                f"Column '{col.name}' has {len(col.synonyms)} synonyms (max 10)",
            ))

    # 10. Placeholder join keys
    if spec.joins:
        for j in _flatten_joins(spec.joins):
            if j.on and "???" in j.on:
                errors.append(YamlValidationError(
                    name,
                    f"Join '{j.name}' has placeholder key — replace '???' with actual column names",
                ))

    return errors


def _flatten_joins(joins: list[JoinDef]) -> list[JoinDef]:
    """Recursively flatten nested joins for validation."""
    result = []
    for j in joins:
        result.append(j)
        if j.joins:
            result.extend(_flatten_joins(j.joins))
    return result


def validate_directory(yaml_dir: str | Path) -> list[YamlValidationError]:
    """Validate all YAML files in a directory."""
    path = Path(yaml_dir)
    all_errors: list[YamlValidationError] = []

    yaml_files = sorted(path.glob("*.yaml")) + sorted(path.glob("*.yml"))
    if not yaml_files:
        return [YamlValidationError(str(path), "No YAML files found in directory")]

    for f in yaml_files:
        all_errors.extend(validate_file(f))

    return all_errors
```

- [ ] **Step 5: Run tests — verify they pass**

```bash
pytest tests/test_validator.py -v
```

Expected: all tests pass — especially `test_unquoted_on_key_gets_warning_not_crash`.

- [ ] **Step 6: Commit**

```bash
git add src/metricviews/validator.py tests/test_validator.py tests/fixtures/
git commit -m "feat: add YAML validator with on-key boolean pre-check"
```

---

## Task 7: Databricks Deployer

**Files:**
- Create: `src/metricviews/deployer.py`
- Create: `tests/test_deployer.py`

- [ ] **Step 1: Write failing tests**

File: `tests/test_deployer.py`

```python
"""Tests for Databricks deployer — all SDK calls mocked."""
from unittest.mock import MagicMock
import pytest
from pathlib import Path
from metricviews.deployer import build_ddl, deploy_file, deploy_directory


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
        with pytest.raises(ValueError, match="\\$\\$"):
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


_VALID_YAML = '''version: "1.1"
source: cat.sch.tbl
dimensions:
  - name: D1
    expr: "col1"
measures:
  - name: M1
    expr: "SUM(col2)"
'''


class TestDeployFile:
    def test_dry_run_returns_sql_without_executing(self, tmp_path):
        f = tmp_path / "test.yaml"
        f.write_text(_VALID_YAML)
        client = MagicMock()

        result = deploy_file(client, f, "cat", "sch", "wh123", dry_run=True)

        assert result.status == "dry_run"
        assert "cat" in result.sql
        assert result.view_fqn == "cat.sch.test"
        client.statement_execution.execute_statement.assert_not_called()

    def test_successful_deploy(self, tmp_path):
        from databricks.sdk.service.sql import StatementState

        f = tmp_path / "test.yaml"
        f.write_text(_VALID_YAML)

        response = MagicMock()
        response.status.state = StatementState.SUCCEEDED

        client = MagicMock()
        client.statement_execution.execute_statement.return_value = response

        result = deploy_file(client, f, "cat", "sch", "wh123")
        assert result.status == "success"

    def test_failed_deploy(self, tmp_path):
        f = tmp_path / "test.yaml"
        f.write_text(_VALID_YAML)

        client = MagicMock()
        client.statement_execution.execute_statement.side_effect = Exception("boom")

        result = deploy_file(client, f, "cat", "sch", "wh123")
        assert result.status == "failed"
        assert "boom" in result.error

    def test_custom_view_name(self, tmp_path):
        f = tmp_path / "test.yaml"
        f.write_text(_VALID_YAML)
        client = MagicMock()

        result = deploy_file(client, f, "cat", "sch", "wh123",
                             view_name="custom_name", dry_run=True)
        assert result.view_fqn == "cat.sch.custom_name"

    def test_deploy_file_validates_before_deploying(self, tmp_path):
        """S3: deploy_file must validate — invalid YAML should fail without executing."""
        bad = tmp_path / "bad.yaml"
        bad.write_text('version: "1.1"\nsource: cat.sch.tbl\ndimensions:\n  - name: D\n    expr: "c"')
        client = MagicMock()

        result = deploy_file(client, bad, "cat", "sch", "wh123")
        assert result.status == "failed"
        assert "Validation" in result.error
        client.statement_execution.execute_statement.assert_not_called()


class TestDeployDirectory:
    def test_skips_files_with_validation_errors(self, tmp_path):
        # Invalid YAML — missing measures
        bad = tmp_path / "bad.yaml"
        bad.write_text('version: "1.1"\nsource: cat.sch.tbl\ndimensions:\n  - name: D\n    expr: "c"')

        client = MagicMock()
        results = deploy_directory(client, tmp_path, "cat", "sch", "wh123")
        assert len(results) == 1
        assert results[0].status == "failed"
        assert "Validation" in results[0].error
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
pytest tests/test_deployer.py -v
```

Expected: `ModuleNotFoundError: No module named 'metricviews.deployer'`

- [ ] **Step 3: Implement deployer.py**

File: `src/metricviews/deployer.py`

```python
"""Deploy metric view YAML files to Databricks Unity Catalog."""
from __future__ import annotations

from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from .models import DeployResult
from .validator import validate_file
import logging
import re
import time

logger = logging.getLogger(__name__)


_IDENTIFIER_PATTERN = re.compile(r"^[\w]+$")


def _validate_identifier(value: str, label: str) -> None:
    """Reject identifiers containing backticks or other unsafe characters."""
    if not _IDENTIFIER_PATTERN.match(value):
        raise ValueError(
            f"Invalid {label} '{value}': must contain only alphanumeric characters and underscores"
        )


def build_ddl(
    yaml_content: str,
    catalog: str,
    schema: str,
    view_name: str,
) -> str:
    """Build the CREATE OR REPLACE VIEW DDL statement.

    Security: validates identifiers and ensures YAML content cannot break
    out of the dollar-quoted block.
    """
    for name, label in [(catalog, "catalog"), (schema, "schema"), (view_name, "view_name")]:
        _validate_identifier(name, label)

    if "$$" in yaml_content:
        raise ValueError(
            "YAML content contains '$$' which would break the DDL dollar-quoting. "
            "Remove '$$' from the YAML file before deploying."
        )

    fqn = f"`{catalog}`.`{schema}`.`{view_name}`"
    return (
        f"CREATE OR REPLACE VIEW {fqn}\n"
        f"WITH METRICS LANGUAGE YAML AS $$\n"
        f"{yaml_content}\n"
        f"$$"
    )


def _view_name_from_path(yaml_path: Path) -> str:
    """'order_metrics.yaml' → 'order_metrics'"""
    return yaml_path.stem


def deploy_file(
    client: WorkspaceClient,
    yaml_path: str | Path,
    catalog: str,
    schema: str,
    warehouse_id: str,
    view_name: str | None = None,
    dry_run: bool = False,
) -> DeployResult:
    """Deploy a single YAML file as a metric view. Validates before deploying."""
    path = Path(yaml_path)
    vname = view_name or _view_name_from_path(path)
    fqn = f"{catalog}.{schema}.{vname}"

    # Validate before deploy — always
    val_errors = [e for e in validate_file(path) if e.severity == "error"]
    if val_errors:
        return DeployResult(
            yaml_file=path.name, view_fqn=fqn,
            status="failed", sql="",
            error=f"Validation failed: {val_errors[0].message}",
        )

    yaml_content = path.read_text()
    ddl = build_ddl(yaml_content, catalog, schema, vname)

    if dry_run:
        logger.info(f"[DRY RUN] Would deploy {path.name} → {fqn}")
        return DeployResult(
            yaml_file=path.name, view_fqn=fqn, status="dry_run", sql=ddl,
        )

    try:
        start = time.monotonic()
        response = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=ddl, wait_timeout="50s",
        )

        if response.status and response.status.state == StatementState.SUCCEEDED:
            elapsed = int((time.monotonic() - start) * 1000)
            logger.info(f"Deployed {path.name} → {fqn} ({elapsed}ms)")
            return DeployResult(
                yaml_file=path.name, view_fqn=fqn, status="success", sql=ddl,
            )
        else:
            error_msg = str(response.status.error) if response.status else "Unknown"
            return DeployResult(
                yaml_file=path.name, view_fqn=fqn,
                status="failed", sql=ddl, error=error_msg,
            )
    except Exception as e:
        return DeployResult(
            yaml_file=path.name, view_fqn=fqn,
            status="failed", sql=ddl, error=str(e),
        )


def deploy_directory(
    client: WorkspaceClient,
    yaml_dir: str | Path,
    catalog: str,
    schema: str,
    warehouse_id: str,
    dry_run: bool = False,
) -> list[DeployResult]:
    """Deploy all YAML files in a directory. Validates (errors only) before each."""
    path = Path(yaml_dir)
    yaml_files = sorted(path.glob("*.yaml")) + sorted(path.glob("*.yml"))

    results: list[DeployResult] = []
    for f in yaml_files:
        errors = [e for e in validate_file(f) if e.severity == "error"]
        if errors:
            results.append(DeployResult(
                yaml_file=f.name, view_fqn="",
                status="failed", sql="",
                error=f"Validation failed: {errors[0].message}",
            ))
            continue
        results.append(deploy_file(client, f, catalog, schema, warehouse_id, dry_run=dry_run))

    return results
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
pytest tests/test_deployer.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/metricviews/deployer.py tests/test_deployer.py
git commit -m "feat: add Databricks deployer"
```

---

## Task 8: CLI + Public API

**Files:**
- Create: `src/metricviews/cli.py`
- Modify: `src/metricviews/__init__.py`
- Create: `tests/test_cli.py`

**Fixes applied:** `--version` flag, SDK error wrapping with human-readable messages.

- [ ] **Step 1: Write failing tests**

File: `tests/test_cli.py`

```python
"""Tests for CLI — uses Click CliRunner, no real Databricks needed."""
import pytest
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
        result = runner.invoke(cli, [
            "validate", str(FIXTURES / "invalid_missing_measures.yaml"),
        ])
        assert result.exit_code != 0

    def test_strict_mode_fails_on_warnings(self):
        runner = CliRunner()
        # with_window_measures.yaml emits experimental warnings
        result = runner.invoke(cli, [
            "validate", str(FIXTURES / "with_window_measures.yaml"), "--strict",
        ])
        assert result.exit_code != 0

    def test_directory_validation(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", str(FIXTURES)])
        # Fixtures dir has invalid files, so should fail
        assert result.exit_code != 0


class TestDeployCommand:
    def test_dry_run_prints_sql(self, tmp_path):
        f = tmp_path / "test.yaml"
        f.write_text((FIXTURES / "sample_orders.yaml").read_text())

        runner = CliRunner()
        result = runner.invoke(cli, [
            "deploy", str(f),
            "--catalog", "dev",
            "--schema", "metrics",
            "--warehouse-id", "wh123",
            "--dry-run",
        ])
        assert result.exit_code == 0
        assert "dry-run" in result.output.lower() or "dry_run" in result.output.lower()


class TestGenerateCommand:
    def test_rejects_bad_source_fqn(self):
        runner = CliRunner()
        result = runner.invoke(cli, [
            "generate", "--source", "not_valid", "--output", "/tmp/test.yaml",
        ])
        assert result.exit_code != 0
        assert "catalog.schema.table" in result.output or "Bad Parameter" in result.output


class TestSdkErrorWrapping:
    @patch("metricviews.cli.introspector")
    def test_auth_failure_shows_friendly_message(self, mock_intro):
        mock_intro.create_client.side_effect = Exception("401 Unauthorized: InvalidAccessToken")
        runner = CliRunner()
        result = runner.invoke(cli, [
            "inspect", "--source", "cat.sch.tbl",
        ])
        assert result.exit_code != 0
        assert "Authentication failed" in result.output

    @patch("metricviews.cli.introspector")
    def test_connection_error_shows_friendly_message(self, mock_intro):
        err = ConnectionError("Failed to connect")
        mock_intro.create_client.side_effect = err
        runner = CliRunner()
        result = runner.invoke(cli, [
            "inspect", "--source", "cat.sch.tbl",
        ])
        assert result.exit_code != 0
        assert "Cannot reach" in result.output or "API error" in result.output
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
pytest tests/test_cli.py -v
```

Expected: `ImportError` (cli.py doesn't exist yet).

- [ ] **Step 3: Implement `__init__.py`**

File: `src/metricviews/__init__.py`

```python
"""uc-metric-views: Generate, validate, and deploy Databricks metric views.

Lazy imports: importing metricviews does not eagerly pull in databricks-sdk
or other heavy dependencies. Use explicit submodule imports (e.g.,
``from metricviews.validator import validate_file``) for lighter usage.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .deployer import deploy_directory as deploy_directory
    from .deployer import deploy_file as deploy_file
    from .generator import spec_from_tables as spec_from_tables
    from .generator import write_yaml_file as write_yaml_file
    from .introspector import create_client as create_client
    from .introspector import discover_table as discover_table
    from .introspector import list_tables as list_tables
    from .validator import validate_directory as validate_directory
    from .validator import validate_file as validate_file

__all__ = [
    "spec_from_tables",
    "write_yaml_file",
    "validate_file",
    "validate_directory",
    "deploy_file",
    "deploy_directory",
    "create_client",
    "discover_table",
    "list_tables",
]


def __getattr__(name: str) -> object:
    """Lazy import public API functions on first access."""
    if name in ("spec_from_tables", "write_yaml_file"):
        from .generator import spec_from_tables, write_yaml_file
        return spec_from_tables if name == "spec_from_tables" else write_yaml_file
    if name in ("validate_file", "validate_directory"):
        from .validator import validate_file, validate_directory
        return validate_file if name == "validate_file" else validate_directory
    if name in ("deploy_file", "deploy_directory"):
        from .deployer import deploy_file, deploy_directory
        return deploy_file if name == "deploy_file" else deploy_directory
    if name in ("create_client", "discover_table", "list_tables"):
        from .introspector import create_client, discover_table, list_tables
        return {"create_client": create_client, "discover_table": discover_table,
                "list_tables": list_tables}[name]
    raise AttributeError(f"module 'metricviews' has no attribute {name!r}")
```

- [ ] **Step 4: Implement cli.py**

File: `src/metricviews/cli.py`

```python
"""CLI entrypoint for uc-metric-views."""
import click
import logging
from pathlib import Path
from importlib.metadata import version as pkg_version
from typing import NoReturn

from . import introspector, generator, validator, deployer
from .heuristics import classify_table

# Module-level logger; basicConfig is called in cli() callback, not at import time
logger = logging.getLogger(__name__)


class DatabricksError(click.ClickException):
    """Wrap SDK exceptions with human-readable messages."""
    pass


def _handle_sdk_error(e: Exception, verbose: bool = False) -> NoReturn:
    """Convert raw SDK exceptions to user-friendly CLI errors. Always raises.

    Raw exception details are only shown when verbose=True to avoid leaking
    tokens or internal paths in CI logs.
    """
    msg = str(e)
    details = f"\nDetails: {msg}" if verbose else ""
    if "401" in msg or "403" in msg or "InvalidAccessToken" in msg:
        raise DatabricksError(
            f"Authentication failed. Check DATABRICKS_HOST and DATABRICKS_TOKEN.{details}"
        )
    if "ConnectionError" in type(e).__name__ or "ConnectTimeout" in msg:
        raise DatabricksError(
            f"Cannot reach Databricks host. Check DATABRICKS_HOST and network.{details}"
        )
    raise DatabricksError(f"Databricks API error: {msg}")


@click.group()
@click.version_option(version=pkg_version("uc-metric-views"), prog_name="ucm")
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging")
@click.pass_context
def cli(ctx: click.Context, verbose: bool) -> None:
    """uc-metric-views: Generate, validate, and deploy Databricks metric views."""
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO, format="%(message)s")
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose


@cli.command()
@click.option("--source", required=True, help="Fully-qualified fact table (catalog.schema.table)")
@click.option("--join", "join_tables", multiple=True, help="FQN of dimension table (repeatable)")
@click.option("--output", required=True, help="Output YAML file path")
@click.option("--overwrite", is_flag=True, help="Overwrite if file exists")
@click.option("--host", default=None, envvar="DATABRICKS_HOST")
@click.option("--token", default=None, envvar="DATABRICKS_TOKEN")
@click.pass_context
def generate(ctx, source, join_tables, output, overwrite, host, token):
    """Scaffold a metric view YAML from a fact table + dimension joins."""
    parts = source.split(".")
    if len(parts) != 3:
        raise click.BadParameter(
            f"must be catalog.schema.table, got '{source}'", param_hint="'--source'"
        )

    try:
        client = introspector.create_client(host, token)
        cat, sch, tbl = parts
        click.echo(f"Introspecting {source}...")
        source_table = introspector.discover_table(client, cat, sch, tbl)
        click.echo(f"  {len(source_table.columns)} columns found")

        dim_tables = []
        for jt in join_tables:
            jparts = jt.split(".")
            if len(jparts) != 3:
                raise click.BadParameter(
                    f"must be catalog.schema.table, got '{jt}'", param_hint="'--join'"
                )
            jcat, jsch, jtbl = jparts
            click.echo(f"Introspecting {jt}...")
            dim = introspector.discover_table(client, jcat, jsch, jtbl)
            dim_tables.append(dim)
            click.echo(f"  {len(dim.columns)} columns found")
    except click.ClickException:
        raise
    except Exception as e:
        _handle_sdk_error(e, verbose=ctx.obj.get("verbose", False))

    spec = generator.spec_from_tables(source_table, dim_tables if dim_tables else None)
    result = generator.write_yaml_file(spec, output, overwrite=overwrite)

    if result:
        click.echo(f"\nGenerated {result}")
        click.echo("Next: review the file, then run 'ucm validate' and 'ucm deploy'")
    else:
        click.echo(f"\nSkipped (file exists). Use --overwrite to replace.")


@cli.command()
@click.option("--source", required=True, help="FQN of table to inspect")
@click.option("--join", "join_tables", multiple=True, help="FQN of dim table (repeatable)")
@click.option("--host", default=None, envvar="DATABRICKS_HOST")
@click.option("--token", default=None, envvar="DATABRICKS_TOKEN")
@click.pass_context
def inspect(ctx, source, join_tables, host, token):
    """Show column inventory and suggested roles. Read-only."""
    try:
        client = introspector.create_client(host, token)
        all_tables = [source] + list(join_tables)
        for fqn in all_tables:
            parts = fqn.split(".")
            if len(parts) != 3:
                raise click.BadParameter(
                    f"must be catalog.schema.table, got '{fqn}'", param_hint="'--source/--join'"
                )

            cat, sch, tbl = parts
            table = introspector.discover_table(client, cat, sch, tbl)
            classified = classify_table(table.columns)

            role_label = "source" if fqn == source else "join"
            click.echo(f"\n{tbl} ({role_label})")
            click.echo("─" * 60)
            click.echo(f"  {'Column':<28} {'Type':<16} {'Suggested Role'}")

            for col in classified:
                role_str = col.role.value if col.role else "?"
                click.echo(f"  {col.name:<28} {col.type_name:<16} → {role_str}")
    except click.ClickException:
        raise
    except Exception as e:
        _handle_sdk_error(e, verbose=ctx.obj.get("verbose", False))


@cli.command()
@click.argument("path", type=click.Path(exists=True))
@click.option("--strict", is_flag=True, help="Treat warnings as errors")
@click.pass_context
def validate(ctx, path, strict):
    """Validate YAML metric view definitions. PATH is a file or directory."""
    p = Path(path)
    errors = (
        validator.validate_directory(p) if p.is_dir()
        else validator.validate_file(p)
    )

    error_count = sum(1 for e in errors if e.severity == "error")
    warning_count = sum(1 for e in errors if e.severity == "warning")

    for e in errors:
        icon = "ERROR" if e.severity == "error" else "WARN "
        click.echo(f"  [{icon}] {e.file}: {e.message}")

    if not errors:
        click.echo("All files valid.")

    if error_count > 0 or (strict and warning_count > 0):
        click.echo(f"\n{error_count} error(s), {warning_count} warning(s)")
        ctx.exit(1)
    else:
        click.echo(f"\n{warning_count} warning(s), 0 errors")


@cli.command()
@click.argument("path", type=click.Path(exists=True))
@click.option("--catalog", required=True, help="Target catalog")
@click.option("--schema", required=True, help="Target schema")
@click.option("--warehouse-id", required=True, envvar="DATABRICKS_WAREHOUSE_ID")
@click.option("--dry-run", is_flag=True, help="Print SQL without executing")
@click.option("--host", default=None, envvar="DATABRICKS_HOST")
@click.option("--token", default=None, envvar="DATABRICKS_TOKEN")
@click.pass_context
def deploy(ctx, path, catalog, schema, warehouse_id, dry_run, host, token):
    """Deploy YAML metric views to Databricks Unity Catalog."""
    try:
        client = introspector.create_client(host, token)
    except Exception as e:
        _handle_sdk_error(e, verbose=ctx.obj.get("verbose", False))

    p = Path(path)
    results = (
        deployer.deploy_directory(client, p, catalog, schema, warehouse_id, dry_run=dry_run)
        if p.is_dir()
        else [deployer.deploy_file(client, p, catalog, schema, warehouse_id, dry_run=dry_run)]
    )

    success = sum(1 for r in results if r.status == "success")
    failed = sum(1 for r in results if r.status == "failed")
    dry = sum(1 for r in results if r.status == "dry_run")

    click.echo(f"\nResults: {success} deployed, {failed} failed, {dry} dry-run")

    if failed > 0:
        for r in results:
            if r.status == "failed":
                click.echo(f"  FAILED: {r.yaml_file} — {r.error}")
        ctx.exit(1)
```

- [ ] **Step 5: Run tests — verify they pass**

```bash
pytest tests/test_cli.py -v
```

Expected: all tests pass.

- [ ] **Step 6: Run full test suite**

```bash
pytest tests/ -v
```

Expected: all tests across all modules pass.

- [ ] **Step 7: Commit**

```bash
git add src/metricviews/__init__.py src/metricviews/cli.py tests/test_cli.py
git commit -m "feat: add CLI with --version and SDK error wrapping"
```

---

## Task 9: Examples for Databricks Free Tier

**Files:**
- Create: `examples/README.md`
- Create: `examples/basic_single_table.yaml`
- Create: `examples/star_schema_joins.yaml`
- Create: `examples/with_semantic_metadata.yaml`

These examples use `samples` catalog tables available on every Databricks workspace (including free Community Edition).

- [ ] **Step 1: Create basic example**

File: `examples/basic_single_table.yaml`

```yaml
# Simplest possible metric view — single source table, no joins.
# Works with Databricks samples catalog (available on all workspaces).
#
# Deploy:
#   ucm deploy examples/basic_single_table.yaml \
#     --catalog your_catalog --schema your_schema --warehouse-id YOUR_ID
#
version: "1.1"
comment: "NYC taxi trip metrics — basic single-table example"
source: samples.nyctaxi.trips

dimensions:
  - name: Pickup Date
    expr: "DATE_TRUNC('DAY', tpep_pickup_datetime)"
    comment: "Day of taxi pickup"

  - name: Pickup Zip
    expr: "pickup_zip"
    comment: "Pickup location ZIP code"

  - name: Dropoff Zip
    expr: "dropoff_zip"
    comment: "Dropoff location ZIP code"

measures:
  - name: Trip Count
    expr: "COUNT(1)"
    comment: "Total number of trips"

  - name: Total Fare
    expr: "SUM(fare_amount)"
    comment: "Sum of base fare amounts"

  - name: Average Distance
    expr: "AVG(trip_distance)"
    comment: "Average trip distance in miles"
```

- [ ] **Step 2: Create star schema example**

File: `examples/star_schema_joins.yaml`

```yaml
# Star schema — fact table joined to a dimension table.
# Uses the TPC-H sample data available in the samples catalog.
#
# Deploy:
#   ucm deploy examples/star_schema_joins.yaml \
#     --catalog your_catalog --schema your_schema --warehouse-id YOUR_ID
#
version: "1.1"
comment: "TPC-H order metrics with customer dimension"
source: samples.tpch.orders

joins:
  - name: customer
    source: samples.tpch.customer
    "on": source.o_custkey = customer.c_custkey

dimensions:
  - name: Order Date
    expr: "o_orderdate"
    comment: "Date the order was placed"

  - name: Order Priority
    expr: "o_orderpriority"
    comment: "Priority level of the order"

  - name: Customer Name
    expr: "customer.c_name"
    comment: "Name of the customer"

  - name: Market Segment
    expr: "customer.c_mktsegment"
    comment: "Customer market segment"

measures:
  - name: Order Count
    expr: "COUNT(1)"
    comment: "Total number of orders"

  - name: Total Revenue
    expr: "SUM(o_totalprice)"
    comment: "Sum of total order prices"

  - name: Average Order Value
    expr: "AVG(o_totalprice)"
    comment: "Average price per order"
```

- [ ] **Step 3: Create semantic metadata example**

File: `examples/with_semantic_metadata.yaml`

```yaml
# Demonstrates v1.1 semantic metadata: display_name, format, synonyms.
# These fields power AI/BI dashboards and Genie discovery.
#
# Deploy:
#   ucm deploy examples/with_semantic_metadata.yaml \
#     --catalog your_catalog --schema your_schema --warehouse-id YOUR_ID
#
version: "1.1"
comment: "TPC-H orders with rich semantic metadata for AI/BI tools"
source: samples.tpch.orders

filter: "o_orderdate >= '1995-01-01'"

dimensions:
  - name: Order Month
    expr: "DATE_TRUNC('MONTH', o_orderdate)"
    display_name: "Order Month"
    comment: "Month the order was placed"
    format:
      type: date
      date_format: year_month_day
    synonyms:
      - "month"
      - "order month"

  - name: Order Priority
    expr: "o_orderpriority"
    display_name: "Priority"
    comment: "Priority level assigned to the order"
    synonyms:
      - "priority"
      - "urgency"

measures:
  - name: Order Count
    expr: "COUNT(1)"
    display_name: "Number of Orders"
    comment: "Total orders in the period"
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
    synonyms:
      - "orders"
      - "order volume"
      - "count"

  - name: Total Revenue
    expr: "SUM(o_totalprice)"
    display_name: "Total Revenue"
    comment: "Sum of all order prices"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "revenue"
      - "total sales"
      - "sales"
```

- [ ] **Step 4: Create examples README**

File: `examples/README.md`

```markdown
# uc-metric-views Examples

Ready-to-use metric view YAML files for trying `uc-metric-views` on any Databricks workspace (including the free Community Edition).

## Prerequisites

1. A Databricks workspace with a SQL warehouse
2. A catalog and schema where you have `CREATE VIEW` permission
3. `uc-metric-views` installed: `pip install uc-metric-views`
4. Auth configured via environment variables:
   ```bash
   export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
   export DATABRICKS_TOKEN="dapi..."
   export DATABRICKS_WAREHOUSE_ID="abc123..."
   ```

## Examples

| File | What it demonstrates |
|------|---------------------|
| `basic_single_table.yaml` | Simplest metric view — NYC taxi data, no joins |
| `star_schema_joins.yaml` | Fact + dimension join (TPC-H orders + customers) |
| `with_semantic_metadata.yaml` | display_name, format, synonyms for AI/BI tools |

## Quick Start

```bash
# 1. Validate all examples
ucm validate ./examples/

# 2. Deploy one example (dry run first)
ucm deploy examples/basic_single_table.yaml \
  --catalog your_catalog \
  --schema your_schema \
  --warehouse-id $DATABRICKS_WAREHOUSE_ID \
  --dry-run

# 3. Deploy for real
ucm deploy examples/basic_single_table.yaml \
  --catalog your_catalog \
  --schema your_schema \
  --warehouse-id $DATABRICKS_WAREHOUSE_ID

# 4. Deploy all examples at once
ucm deploy examples/ \
  --catalog your_catalog \
  --schema your_schema \
  --warehouse-id $DATABRICKS_WAREHOUSE_ID
```

## Sample Data Sources

These examples use datasets available on every Databricks workspace:

- **`samples.nyctaxi.trips`** — NYC taxi trip records
- **`samples.tpch.orders`** — TPC-H benchmark order data
- **`samples.tpch.customer`** — TPC-H benchmark customer data
```

- [ ] **Step 5: Validate examples pass**

```bash
ucm validate ./examples/
```

Expected: 0 errors (may have warnings for no aggregate function check on `AVG`/`COUNT` — that's fine).

- [ ] **Step 6: Commit**

```bash
git add examples/
git commit -m "docs: add example metric views for Databricks free tier"
```

---

## Task 10: README

**Files:**
- Create: `README.md`

- [ ] **Step 1: Write README**

File: `README.md`

```markdown
# uc-metric-views

Generate, validate, and deploy [Databricks Unity Catalog metric views](https://docs.databricks.com/en/sql/language-manual/sql-ref-metric-views.html) from the command line.

## What it does

A metric view is a YAML definition that describes business metrics — dimensions, measures, joins — on top of Unity Catalog tables. `uc-metric-views` automates the lifecycle:

1. **Generate** — introspect UC tables, classify columns as dimensions or measures via heuristics, and scaffold a starter YAML file
2. **Validate** — check YAML files against the Databricks metric view spec before deployment
3. **Deploy** — wrap YAML in DDL and execute via the Databricks SDK

## Install

```bash
pip install uc-metric-views
```

Requires Python 3.10+.

## Quick Start

```bash
# Set up auth (or use ~/.databrickscfg)
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."

# Generate a metric view from a table
ucm generate \
  --source analytics.gold.fct_orders \
  --join analytics.gold.dim_customer \
  --output ./metric_views/order_metrics.yaml

# Review and edit the generated YAML, then validate
ucm validate ./metric_views/ --strict

# Deploy (dry run first)
ucm deploy ./metric_views/ \
  --catalog analytics_dev \
  --schema metrics \
  --warehouse-id $DATABRICKS_WAREHOUSE_ID \
  --dry-run

# Deploy for real
ucm deploy ./metric_views/ \
  --catalog analytics_dev \
  --schema metrics \
  --warehouse-id $DATABRICKS_WAREHOUSE_ID
```

## CLI Commands

| Command | Purpose |
|---------|---------|
| `ucm generate` | Scaffold a metric view YAML from UC table schemas |
| `ucm inspect` | Show column inventory and suggested roles (read-only) |
| `ucm validate` | Validate YAML files against the metric view spec |
| `ucm deploy` | Deploy YAML files as metric views to Unity Catalog |

Run `ucm --help` or `ucm <command> --help` for full options.

## How Generation Works

`ucm generate` connects to Databricks, reads column metadata, and classifies each column:

| Pattern | Example | Classification |
|---------|---------|----------------|
| `*_id`, `*_key`, `*_date`, `is_*` | `customer_id`, `order_date` | Dimension |
| `*_amount`, `*_price`, `*_count` | `total_amount`, `unit_price` | Measure |
| `_*`, `etl_*`, `*_loaded_at` | `_fivetran_synced` | Ignored |
| No name match → type fallback | `STRING` → dim, `DOUBLE` → measure | Type-based |

The generated YAML is a starting point. Review it, fix any mis-classifications, and add semantic metadata (display names, formats, synonyms) for AI/BI tools.

## Examples

See the [`examples/`](examples/) directory for ready-to-use metric views that work with Databricks sample datasets (available on all workspaces including Community Edition).

## Python API

```python
from metricviews import create_client, discover_table, spec_from_tables, write_yaml_file
from metricviews import validate_file, deploy_file

# Generate
client = create_client()
source = discover_table(client, "cat", "schema", "fct_orders")
spec = spec_from_tables(source)
write_yaml_file(spec, "./metric_views/orders.yaml")

# Validate
errors = validate_file("./metric_views/orders.yaml")

# Deploy
result = deploy_file(client, "./metric_views/orders.yaml",
                     catalog="dev", schema="metrics", warehouse_id="abc123")
```

## CI/CD

`uc-metric-views` ships with GitHub Actions workflows for automated validation and deployment. See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full CI/CD setup.

## Metric View YAML Spec

`uc-metric-views` supports the complete Databricks metric view YAML spec v1.1, including:

- Dimensions and measures (required)
- Star schema and snowflake joins
- SQL filters
- Semantic metadata (display_name, format, synonyms)
- Window measures (experimental)
- Materialization (experimental)

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full spec reference.

## Roadmap

See [`ROADMAP.md`](ROADMAP.md) for planned features.

## License

MIT
```

- [ ] **Step 2: Create ROADMAP.md**

`ROADMAP.md` already exists at the repo root (created during planning). Verify it contains the deferred features (v0.2 DX, v0.3 Ecosystem, v0.4 Advanced Generation, Not Planned). If missing, copy from `ROADMAP.md` in the repo root.

- [ ] **Step 3: Commit**

```bash
git add README.md ROADMAP.md
git commit -m "docs: add README and ROADMAP"
```

---

## Task 11: GitHub Actions

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `.github/workflows/deploy.yml`

**Fix applied:** CD workflow installs from checkout (`pip install -e .`), not PyPI.

- [ ] **Step 1: Create CI workflow**

File: `.github/workflows/ci.yml`

```yaml
name: CI
on:
  pull_request:
    paths: ["src/**", "tests/**", "metric_views/**", "examples/**", "pyproject.toml"]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.10", "3.11", "3.12", "3.13"]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}
      - run: pip install -e ".[dev]"
      - name: Lint
        run: ruff check src/ tests/
      - name: Format check
        run: ruff format --check src/ tests/
      - name: Type check
        run: mypy src/
      - name: Unit tests
        run: pytest tests/ -v --cov=metricviews --cov-report=term

  validate:
    runs-on: ubuntu-latest
    if: hashFiles('metric_views/*.yaml') != ''
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -e .
      - run: ucm validate ./metric_views/ --strict
```

- [ ] **Step 2: Create CD workflow**

File: `.github/workflows/deploy.yml`

```yaml
name: Deploy Metric Views
on:
  push:
    branches: [main]
    paths: ["metric_views/**"]
  workflow_dispatch:
    inputs:
      target:
        description: "Target environment"
        required: true
        type: choice
        options: [dev, staging, prod]
      dry_run:
        description: "Dry run only"
        type: boolean
        default: false

jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: ${{ github.event.inputs.target || 'dev' }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -e .
      - name: Validate
        run: ucm validate ./metric_views/ --strict
      - name: Deploy
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
          DATABRICKS_WAREHOUSE_ID: ${{ vars.WAREHOUSE_ID }}
          DEPLOY_CATALOG: ${{ vars.CATALOG }}
          DEPLOY_SCHEMA: ${{ vars.SCHEMA }}
        run: |
          ucm deploy ./metric_views/ \
            --catalog "$DEPLOY_CATALOG" \
            --schema "$DEPLOY_SCHEMA" \
            ${{ github.event.inputs.dry_run == 'true' && '--dry-run' || '' }}
```

- [ ] **Step 3: Commit**

```bash
git add .github/
git commit -m "ci: add CI and CD workflows"
```

---

## Task 12: Final Verification

- [ ] **Step 1: Run full test suite**

```bash
pytest tests/ -v --cov=metricviews --cov-report=term
```

Expected: all tests pass, reasonable coverage (>80%).

- [ ] **Step 2: Run linting and format check**

```bash
ruff check src/ tests/
ruff format --check src/ tests/
```

Expected: no errors.

- [ ] **Step 3: Run type checking**

```bash
mypy src/
```

Expected: no errors (or only minor strict-mode issues to fix).

- [ ] **Step 4: Validate examples**

```bash
ucm validate ./examples/
```

Expected: 0 errors.

- [ ] **Step 5: Verify CLI version**

```bash
ucm --version
```

Expected: `ucm, version 0.1.0`

- [ ] **Step 6: Fix any issues found in steps 1-5, then commit**

```bash
git add -A
git commit -m "chore: fix lint and type issues for v0.1.0"
```

- [ ] **Step 7: Tag release**

```bash
git tag v0.1.0
```
