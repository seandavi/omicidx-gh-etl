"""
CLI commands for warehouse operations.
"""

import click
from pathlib import Path
from loguru import logger
import sys

from omicidx_etl.transformations.warehouse import (
    WarehouseConfig,
    WarehouseConnection,
    run_warehouse,
    discover_models
)
from omicidx_etl.transformations.config import load_config, print_config


@click.group()
def warehouse():
    """Data warehouse operations and management."""
    pass


@warehouse.command()
@click.option(
    '--config',
    default='warehouse.yml',
    help='Path to config file'
)
def show_config(config):
    """Show current warehouse configuration."""
    warehouse_config = load_config(config)
    print_config(warehouse_config)


@warehouse.command()
@click.option(
    '--config',
    default='warehouse.yml',
    help='Path to config file (default: warehouse.yml)'
)
@click.option(
    '--models',
    multiple=True,
    help='Specific models to run (can specify multiple times)'
)
@click.option(
    '--fail-fast/--no-fail-fast',
    default=True,
    help='Stop on first error'
)
def run(config, models, fail_fast):
    """Run warehouse transformations."""
    # Load config from file + environment variables
    warehouse_config = load_config(config)

    logger.info(f"Running warehouse: {warehouse_config.db_path}")
    logger.info(f"Export directory: {warehouse_config.export_dir}")

    results = run_warehouse(
        config=warehouse_config,
        models=list(models) if models else None,
        fail_fast=fail_fast
    )

    # Print summary
    success = sum(1 for r in results if r['status'] == 'success')
    errors = sum(1 for r in results if r['status'] == 'error')

    click.echo(f"\n{'='*60}")
    click.echo(f"Warehouse run complete:")
    click.echo(f"  Success: {success}")
    click.echo(f"  Errors:  {errors}")
    click.echo(f"{'='*60}")

    # Exit with error if any failed
    if errors > 0:
        sys.exit(1)


@warehouse.command()
@click.option(
    '--db-path',
    default='omicidx_warehouse.duckdb',
    help='Path to DuckDB warehouse database'
)
@click.option(
    '--models-dir',
    default='omicidx_etl/transformations/models',
    help='Path to models directory'
)
def plan(db_path, models_dir):
    """Show execution plan without running."""
    config = WarehouseConfig(
        db_path=db_path,
        models_dir=models_dir
    )

    logger.info("Generating execution plan")

    results = run_warehouse(
        config=config,
        dry_run=True
    )

    click.echo(f"\n{'='*60}")
    click.echo(f"Execution plan ({len(results)} models):")
    click.echo(f"{'='*60}")

    for i, result in enumerate(results, 1):
        click.echo(f"{i}. {result['model_name']}")


@warehouse.command()
@click.option(
    '--models-dir',
    default='omicidx_etl/transformations/models',
    help='Path to models directory'
)
def list_models(models_dir):
    """List all discovered models."""
    models = discover_models(models_dir)

    click.echo(f"\n{'='*60}")
    click.echo(f"Discovered {len(models)} models:")
    click.echo(f"{'='*60}\n")

    # Group by layer
    by_layer = {}
    for model in models:
        if model.layer not in by_layer:
            by_layer[model.layer] = []
        by_layer[model.layer].append(model)

    for layer in ['raw', 'staging', 'mart']:
        if layer not in by_layer:
            continue

        click.echo(f"{layer.upper()}:")
        for model in by_layer[layer]:
            deps = f" (depends on: {', '.join(model.depends_on)})" if model.depends_on else ""
            click.echo(f"  - {model.name}{deps}")
        click.echo()


@warehouse.command()
@click.option(
    '--db-path',
    default='omicidx_warehouse.duckdb',
    help='Path to DuckDB warehouse database'
)
def init(db_path):
    """Initialize warehouse database and schemas."""
    config = WarehouseConfig(db_path=db_path)

    logger.info(f"Initializing warehouse: {db_path}")

    with WarehouseConnection(config) as conn:
        # Schemas are created automatically in __enter__
        click.echo(f"\nWarehouse initialized: {db_path}")
        click.echo("Schemas created: raw, staging, mart, meta")


@warehouse.command()
@click.option(
    '--db-path',
    default='omicidx_warehouse.duckdb',
    help='Path to DuckDB warehouse database'
)
@click.option(
    '--limit',
    default=20,
    help='Number of recent runs to show'
)
def history(db_path, limit):
    """Show recent model run history."""
    config = WarehouseConfig(db_path=db_path)

    with WarehouseConnection(config) as conn:
        results = conn.execute(f"""
            SELECT
                run_id,
                model_name,
                layer,
                started_at,
                status,
                execution_time_seconds,
                rows_affected,
                error_message
            FROM meta.model_runs
            ORDER BY started_at DESC
            LIMIT {limit}
        """).fetchall()

        click.echo(f"\n{'='*80}")
        click.echo(f"Recent model runs (last {limit}):")
        click.echo(f"{'='*80}\n")

        if not results:
            click.echo("No runs found.")
            return

        # Print table
        click.echo(f"{'Model':<30} {'Status':<10} {'Duration':<12} {'Rows':<10} {'Started':<20}")
        click.echo(f"{'-'*30} {'-'*10} {'-'*12} {'-'*10} {'-'*20}")

        for row in results:
            model = f"{row[2]}.{row[1]}"
            status = row[4]
            duration = f"{row[5]:.2f}s" if row[5] else "N/A"
            rows = str(row[6]) if row[6] else "N/A"
            started = str(row[3])[:19] if row[3] else "N/A"

            # Color status
            if status == 'success':
                status_colored = click.style(status, fg='green')
            elif status == 'error':
                status_colored = click.style(status, fg='red')
            else:
                status_colored = status

            click.echo(f"{model:<30} {status_colored:<10} {duration:<12} {rows:<10} {started:<20}")

            # Show error if present
            if row[7]:
                click.echo(f"  Error: {row[7]}")


@warehouse.command()
@click.option(
    '--db-path',
    default='omicidx_warehouse.duckdb',
    help='Path to DuckDB warehouse database'
)
@click.option(
    '--layer',
    type=click.Choice(['raw', 'staging', 'mart', 'all']),
    default='all',
    help='Show tables from specific layer'
)
def tables(db_path, layer):
    """List all tables and views in warehouse."""
    config = WarehouseConfig(db_path=db_path)

    with WarehouseConnection(config) as conn:
        if layer == 'all':
            schemas = ['raw', 'staging', 'mart']
        else:
            schemas = [layer]

        click.echo(f"\n{'='*80}")
        click.echo("Warehouse tables and views:")
        click.echo(f"{'='*80}\n")

        for schema in schemas:
            results = conn.execute(f"""
                SELECT
                    table_name,
                    table_type
                FROM information_schema.tables
                WHERE table_schema = '{schema}'
                ORDER BY table_name
            """).fetchall()

            if results:
                click.echo(f"{schema.upper()}:")
                for row in results:
                    table_name = row[0]
                    table_type = row[1]
                    click.echo(f"  - {table_name} ({table_type})")
                click.echo()


@warehouse.command()
@click.option(
    '--db-path',
    default='omicidx_warehouse.duckdb',
    help='Path to DuckDB warehouse database'
)
@click.argument('model_name')
def describe(db_path, model_name):
    """Show documentation for a specific model."""
    config = WarehouseConfig(db_path=db_path)

    with WarehouseConnection(config) as conn:
        # Get documentation
        result = conn.execute("""
            SELECT description, columns, tags
            FROM meta.model_docs
            WHERE model_name = ?
        """, [model_name]).fetchone()

        if not result:
            click.echo(f"Model not found: {model_name}")
            return

        click.echo(f"\n{'='*80}")
        click.echo(f"Model: {model_name}")
        click.echo(f"{'='*80}\n")

        # Description
        if result[0]:
            click.echo("Description:")
            click.echo(result[0])
            click.echo()

        # Tags
        if result[2]:
            click.echo(f"Tags: {result[2]}")
            click.echo()

        # Get column info from actual table
        layer = conn.execute("""
            SELECT layer FROM meta.model_docs WHERE model_name = ?
        """, [model_name]).fetchone()[0]

        try:
            columns = conn.execute(f"""
                DESCRIBE {layer}.{model_name}
            """).fetchall()

            click.echo("Columns:")
            for col in columns:
                click.echo(f"  - {col[0]}: {col[1]}")
        except:
            pass


if __name__ == "__main__":
    warehouse()
