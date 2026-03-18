"""Tests for Unity Catalog introspector — all SDK calls mocked."""
from unittest.mock import MagicMock, patch

from uc_metrics.introspector import create_client, discover_table, list_tables


class TestCreateClient:
    @patch("uc_metrics.introspector.WorkspaceClient")
    def test_creates_client_with_explicit_args(self, mock_ws):
        create_client(host="https://test.cloud.databricks.com", token="dapi123")
        mock_ws.assert_called_once_with(
            host="https://test.cloud.databricks.com", token="dapi123",
        )

    @patch("uc_metrics.introspector.WorkspaceClient")
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
