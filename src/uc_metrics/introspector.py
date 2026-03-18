"""Unity Catalog introspection — discover tables and their schemas."""
from __future__ import annotations

import fnmatch
import logging

from databricks.sdk import WorkspaceClient

from .models import DiscoveredColumn, DiscoveredTable

logger = logging.getLogger(__name__)


def create_client(
    host: str | None = None,
    token: str | None = None,
) -> WorkspaceClient:
    """Create a Databricks SDK client.

    Falls back to DATABRICKS_HOST / DATABRICKS_TOKEN env vars
    and ~/.databrickscfg profile if not provided.
    """
    kwargs: dict[str, str] = {}
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

    # col.type_name is a ColumnTypeName enum — extract .value for the string
    columns = [
        DiscoveredColumn(
            name=col.name,
            type_name=col.type_name.value if col.type_name else "STRING",
            comment=col.comment,
        )
        for col in (table_info.columns or [])
    ]

    return DiscoveredTable(
        catalog=catalog,
        schema_name=schema,
        table_name=table_name,
        columns=columns,
        comment=table_info.comment,
    )
