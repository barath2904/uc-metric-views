"""Tests for metric view YAML generation."""

from pathlib import Path

from metricviews.generator import spec_from_tables, spec_to_yaml, write_yaml_file
from metricviews.models import (
    DimensionDef,
    DiscoveredColumn,
    DiscoveredTable,
    MeasureDef,
    MetricViewSpec,
)


def _make_source() -> DiscoveredTable:
    return DiscoveredTable(
        catalog="cat",
        schema_name="sch",
        table_name="fct_orders",
        columns=[
            DiscoveredColumn(name="order_id", type_name="BIGINT"),
            DiscoveredColumn(name="order_date", type_name="DATE"),
            DiscoveredColumn(name="total_amount", type_name="DECIMAL(10,2)"),
            DiscoveredColumn(name="_etl_loaded_at", type_name="TIMESTAMP"),
        ],
    )


def _make_dim() -> DiscoveredTable:
    return DiscoveredTable(
        catalog="cat",
        schema_name="sch",
        table_name="dim_customer",
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
            catalog="cat",
            schema_name="sch",
            table_name="fct",
            columns=[
                DiscoveredColumn(name="customer_id", type_name="BIGINT"),
                DiscoveredColumn(name="amount", type_name="DECIMAL"),
            ],
        )
        dim = DiscoveredTable(
            catalog="cat",
            schema_name="sch",
            table_name="dim_customer",
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
            catalog="cat",
            schema_name="sch",
            table_name="fct",
            columns=[DiscoveredColumn(name="amount", type_name="DECIMAL")],
        )
        dim = DiscoveredTable(
            catalog="cat",
            schema_name="sch",
            table_name="dim_other",
            columns=[DiscoveredColumn(name="name", type_name="STRING")],
        )
        spec = spec_from_tables(source, [dim])
        assert spec.joins is not None
        assert "???" in spec.joins[0].on  # type: ignore[index]

    def test_dim_table_columns_become_dimensions(self):
        spec = spec_from_tables(_make_source(), [_make_dim()])
        dim_names = [d.name for d in spec.dimensions]
        assert "Customer Name" in dim_names
        # customer_id is a key column — should NOT appear as a dimension from the dim table
        assert "Customer Id" not in dim_names


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
            joins=[
                JoinDef(
                    name="customer",
                    source="cat.sch.customer",
                    on="source.cust_key = customer.cust_key",
                    joins=[
                        JoinDef(
                            name="nation",
                            source="cat.sch.nation",
                            on="customer.nation_key = nation.nation_key",
                        )
                    ],
                )
            ],
            dimensions=[DimensionDef(name="D1", expr="col1")],
            measures=[MeasureDef(name="M1", expr="SUM(col2)")],
        )
        yaml_str = spec_to_yaml(spec)
        assert "      - name: nation" in yaml_str
        assert '"on": customer.nation_key' in yaml_str


class TestWriteYamlFile:
    def test_creates_file(self, tmp_path: Path):
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

    def test_skips_existing_file_without_overwrite(self, tmp_path: Path):
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

    def test_overwrites_when_flag_set(self, tmp_path: Path):
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

    def test_creates_parent_dirs(self, tmp_path: Path):
        spec = MetricViewSpec(
            source="cat.sch.tbl",
            dimensions=[DimensionDef(name="D1", expr="col1")],
            measures=[MeasureDef(name="M1", expr="SUM(col2)")],
        )
        out = tmp_path / "deep" / "nested" / "test.yaml"
        result = write_yaml_file(spec, out)
        assert result == out
        assert out.exists()
