"""CLI entrypoint for uc-metric-views."""

from __future__ import annotations

import logging
from importlib.metadata import version as pkg_version
from pathlib import Path
from typing import NoReturn

import click

from . import deployer, generator, introspector, validator
from .heuristics import classify_table


class DatabricksError(click.ClickException):
    """Wrap SDK exceptions with human-readable messages."""


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
    if details:
        raise DatabricksError(f"Databricks API error.{details}")
    raise DatabricksError("Databricks API error. Use --verbose for details.")


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
def generate(
    ctx: click.Context,
    source: str,
    join_tables: tuple[str, ...],
    output: str,
    overwrite: bool,
    host: str | None,
    token: str | None,
) -> None:
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
        click.echo("\nSkipped (file exists). Use --overwrite to replace.")


@cli.command()
@click.option("--source", required=True, help="FQN of table to inspect")
@click.option("--join", "join_tables", multiple=True, help="FQN of dim table (repeatable)")
@click.option("--host", default=None, envvar="DATABRICKS_HOST")
@click.option("--token", default=None, envvar="DATABRICKS_TOKEN")
@click.pass_context
def inspect(
    ctx: click.Context,
    source: str,
    join_tables: tuple[str, ...],
    host: str | None,
    token: str | None,
) -> None:
    """Show column inventory and suggested roles. Read-only."""
    try:
        client = introspector.create_client(host, token)
        all_tables = [source, *list(join_tables)]
        for fqn in all_tables:
            parts = fqn.split(".")
            if len(parts) != 3:
                raise click.BadParameter(
                    f"must be catalog.schema.table, got '{fqn}'",
                    param_hint="'--source/--join'",
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
def validate(ctx: click.Context, path: str, strict: bool) -> None:
    """Validate YAML metric view definitions. PATH is a file or directory."""
    p = Path(path)
    errors = validator.validate_directory(p) if p.is_dir() else validator.validate_file(p)

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
@click.option(
    "--warehouse-id",
    default=None,
    envvar="DATABRICKS_WAREHOUSE_ID",
    help="SQL warehouse ID (required unless --dry-run)",
)
@click.option("--dry-run", is_flag=True, help="Print SQL without executing")
@click.option("--host", default=None, envvar="DATABRICKS_HOST")
@click.option("--token", default=None, envvar="DATABRICKS_TOKEN")
@click.pass_context
def deploy(
    ctx: click.Context,
    path: str,
    catalog: str,
    schema: str,
    warehouse_id: str | None,
    dry_run: bool,
    host: str | None,
    token: str | None,
) -> None:
    """Deploy YAML metric views to Databricks Unity Catalog."""
    if not dry_run and not warehouse_id:
        raise click.UsageError("--warehouse-id is required unless --dry-run is set")

    if dry_run:
        client = None  # not used in dry_run path
        wh_id = warehouse_id or ""
    else:
        try:
            client = introspector.create_client(host, token)
        except Exception as e:
            _handle_sdk_error(e, verbose=ctx.obj.get("verbose", False))
        wh_id = warehouse_id or ""  # validated above

    p = Path(path)
    results = (
        deployer.deploy_directory(client, p, catalog, schema, wh_id, dry_run=dry_run)  # type: ignore[arg-type]
        if p.is_dir()
        else [deployer.deploy_file(client, p, catalog, schema, wh_id, dry_run=dry_run)]  # type: ignore[arg-type]
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
