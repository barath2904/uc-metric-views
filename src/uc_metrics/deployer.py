"""Deploy metric view YAML files to Databricks Unity Catalog."""
from __future__ import annotations

import logging
import re
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

from .models import DeployResult
from .validator import validate_file

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
