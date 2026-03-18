"""uc-metrics: Generate, validate, and deploy Databricks metric views.

Lazy imports: importing uc_metrics does not eagerly pull in databricks-sdk
or other heavy dependencies. Use explicit submodule imports (e.g.,
``from uc_metrics.validator import validate_file``) for lighter usage.
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
    "create_client",
    "deploy_directory",
    "deploy_file",
    "discover_table",
    "list_tables",
    "spec_from_tables",
    "validate_directory",
    "validate_file",
    "write_yaml_file",
]


def __getattr__(name: str) -> object:
    """Lazy import public API functions on first access."""
    if name in ("spec_from_tables", "write_yaml_file"):
        from .generator import spec_from_tables, write_yaml_file
        return spec_from_tables if name == "spec_from_tables" else write_yaml_file
    if name in ("validate_file", "validate_directory"):
        from .validator import validate_directory, validate_file
        return validate_file if name == "validate_file" else validate_directory
    if name in ("deploy_file", "deploy_directory"):
        from .deployer import deploy_directory, deploy_file
        return deploy_file if name == "deploy_file" else deploy_directory
    if name in ("create_client", "discover_table", "list_tables"):
        from .introspector import create_client, discover_table, list_tables
        return {"create_client": create_client, "discover_table": discover_table,
                "list_tables": list_tables}[name]
    raise AttributeError(f"module 'uc_metrics' has no attribute {name!r}")
