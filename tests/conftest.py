"""Shared pytest fixtures for uc-metric-views tests."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from metricviews.models import DiscoveredColumn, DiscoveredTable


@pytest.fixture()
def source_table() -> DiscoveredTable:
    """Standard fact table for generation/inspection tests."""
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


@pytest.fixture()
def dim_table() -> DiscoveredTable:
    """Standard dimension table for join tests."""
    return DiscoveredTable(
        catalog="cat",
        schema_name="sch",
        table_name="dim_customer",
        columns=[
            DiscoveredColumn(name="customer_id", type_name="BIGINT"),
            DiscoveredColumn(name="customer_name", type_name="STRING"),
        ],
    )


@pytest.fixture()
def mock_workspace_client() -> MagicMock:
    """Pre-configured mock of databricks.sdk.WorkspaceClient."""
    return MagicMock()
