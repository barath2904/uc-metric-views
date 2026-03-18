"""Tests for Pydantic domain models."""
import pydantic
import pytest

from uc_metrics.models import (
    DeployResult,
    DimensionDef,
    DiscoveredColumn,
    DiscoveredTable,
    JoinDef,
    MaterializationConfig,
    MaterializedViewDef,
    MeasureDef,
    MetricViewSpec,
    WindowSpec,
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
