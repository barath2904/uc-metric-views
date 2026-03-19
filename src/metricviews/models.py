"""Pydantic models for Databricks metric view YAML spec v1.1."""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


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
    """A groupable attribute (column or expression) in the metric view."""

    name: str
    expr: str
    comment: str | None = None
    display_name: str | None = None
    format: dict[str, Any] | None = None
    synonyms: list[str] | None = None


class MeasureDef(BaseModel):
    """An aggregate expression (metric) in the metric view."""

    name: str
    expr: str
    comment: str | None = None
    display_name: str | None = None
    format: dict[str, Any] | None = None
    synonyms: list[str] | None = None
    window: list[WindowSpec] | None = None


class JoinDef(BaseModel):
    """A dimension table joined into the metric view, with optional nested joins."""

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
    """A single pre-aggregated view within a materialization config."""

    name: str
    type: str
    dimensions: list[str] | None = None
    measures: list[str] | None = None


class MaterializationConfig(BaseModel):
    """Experimental — schedule and mode for pre-aggregating the metric view."""

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
