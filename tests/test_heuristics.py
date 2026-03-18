"""Tests for column classification heuristics."""
import pytest

from uc_metrics.heuristics import classify_column, classify_table, suggest_aggregation
from uc_metrics.models import ColumnRole, DiscoveredColumn


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
