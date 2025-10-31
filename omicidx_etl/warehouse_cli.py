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
from omicidx_etl.transformations.config import (
    load_config,
    load_deployment_config,
    print_config
)
from omicidx_etl.transformations.deploy import (
    generate_catalog,
    create_remote_database,
    upload_to_r2,
    upload_directory_to_r2
)


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
    deployment_config = load_deployment_config(config)
    print_config(warehouse_config, deployment_config)


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


# ============================================================================
# Deployment Commands
# ============================================================================

@warehouse.group()
def deploy():
    """Deployment commands for publishing to cloud storage."""
    pass


@deploy.command('catalog')
@click.option(
    '--config',
    default='warehouse.yml',
    help='Path to config file'
)
@click.option(
    '--output',
    help='Output path for catalog.json (default: export_dir/catalog.json)'
)
def deploy_catalog(config, output):
    """Generate catalog.json from schema files and exports."""
    warehouse_config = load_config(config)
    deployment_config = load_deployment_config(config)

    if not deployment_config:
        click.echo("Error: Deployment not configured. Add 'deployment:' section to warehouse.yml", err=True)
        sys.exit(1)

    # Determine output path
    if output is None:
        output = Path(warehouse_config.export_dir) / deployment_config.catalog_path

    output = Path(output)

    # Generate catalog
    logger.info("Generating catalog...")
    catalog = generate_catalog(warehouse_config, deployment_config, output)

    click.echo(f"\n{'='*60}")
    click.echo(f"Catalog generated successfully!")
    click.echo(f"  Tables:     {len(catalog['tables'])}")
    click.echo(f"  Output:     {output}")
    click.echo(f"{'='*60}")


@deploy.command('database')
@click.option(
    '--config',
    default='warehouse.yml',
    help='Path to config file'
)
@click.option(
    '--output',
    help='Output path for database (default: export_dir/database_path)'
)
def deploy_database(config, output):
    """Create remote views database pointing to exported parquet files."""
    warehouse_config = load_config(config)
    deployment_config = load_deployment_config(config)

    if not deployment_config:
        click.echo("Error: Deployment not configured. Add 'deployment:' section to warehouse.yml", err=True)
        sys.exit(1)

    # First generate catalog
    logger.info("Generating catalog...")
    catalog = generate_catalog(warehouse_config, deployment_config)

    # Create remote database
    logger.info("Creating remote views database...")
    if output:
        db_path = Path(output)
    else:
        db_path = Path(warehouse_config.export_dir) / deployment_config.database_path

    db_path = create_remote_database(warehouse_config, deployment_config, catalog, db_path)

    click.echo(f"\n{'='*60}")
    click.echo(f"Remote views database created!")
    click.echo(f"  Database:   {db_path}")
    click.echo(f"  Views:      {sum(1 for t in catalog['tables'].values() if 'export' in t)}")
    click.echo(f"{'='*60}")


@deploy.command('upload')
@click.option(
    '--config',
    default='warehouse.yml',
    help='Path to config file'
)
@click.option(
    '--data/--no-data',
    default=True,
    help='Upload exported data files'
)
@click.option(
    '--catalog/--no-catalog',
    default=True,
    help='Upload catalog.json'
)
@click.option(
    '--database/--no-database',
    default=True,
    help='Upload remote views database'
)
@click.option(
    '--dry-run',
    is_flag=True,
    help='Show what would be uploaded without uploading'
)
def deploy_upload(config, data, catalog, database, dry_run):
    """Upload data warehouse artifacts to R2/S3."""
    warehouse_config = load_config(config)
    deployment_config = load_deployment_config(config)

    if not deployment_config:
        click.echo("Error: Deployment not configured. Add 'deployment:' section to warehouse.yml", err=True)
        sys.exit(1)

    if dry_run:
        click.echo("\nDry run - no files will be uploaded\n")

    export_dir = Path(warehouse_config.export_dir)
    files_to_upload = []

    # Collect data files
    if data:
        data_files = list(export_dir.glob('**/*.parquet'))
        for file_path in data_files:
            relative_path = file_path.relative_to(export_dir)
            remote_path = f"{deployment_config.data_prefix}/{relative_path}"
            files_to_upload.append((file_path, remote_path, 'data'))

    # Collect catalog
    if catalog:
        catalog_path = export_dir / deployment_config.catalog_path
        if catalog_path.exists():
            files_to_upload.append((catalog_path, deployment_config.catalog_path, 'catalog'))
        else:
            click.echo(f"Warning: Catalog not found: {catalog_path}", err=True)

    # Collect database
    if database:
        db_path = export_dir / deployment_config.database_path
        if db_path.exists():
            files_to_upload.append((db_path, deployment_config.database_path, 'database'))
        else:
            click.echo(f"Warning: Database not found: {db_path}", err=True)

    if not files_to_upload:
        click.echo("No files to upload.")
        sys.exit(0)

    # Show what will be uploaded
    click.echo(f"\n{'='*80}")
    click.echo(f"Files to upload to {deployment_config.bucket_name}:")
    click.echo(f"{'='*80}\n")

    total_size = 0
    for local_path, remote_path, file_type in files_to_upload:
        size_mb = local_path.stat().st_size / 1024 / 1024
        total_size += size_mb
        click.echo(f"  [{file_type:8}] {remote_path:<50} ({size_mb:.2f} MB)")

    click.echo(f"\nTotal: {len(files_to_upload)} files ({total_size:.2f} MB)")

    if dry_run:
        return

    # Upload files
    click.echo("\nUploading files...")
    success_count = 0
    error_count = 0

    for local_path, remote_path, file_type in files_to_upload:
        logger.info(f"Uploading {remote_path}...")
        success = upload_to_r2(local_path, deployment_config, remote_path)
        if success:
            success_count += 1
            click.echo(f"  ✓ {remote_path}")
        else:
            error_count += 1
            click.echo(f"  ✗ {remote_path}", err=True)

    # Summary
    click.echo(f"\n{'='*60}")
    click.echo(f"Upload complete:")
    click.echo(f"  Success: {success_count}")
    click.echo(f"  Errors:  {error_count}")
    click.echo(f"{'='*60}")

    if error_count > 0:
        sys.exit(1)


@deploy.command('all')
@click.option(
    '--config',
    default='warehouse.yml',
    help='Path to config file'
)
@click.option(
    '--skip-upload',
    is_flag=True,
    help='Skip upload step'
)
def deploy_all(config, skip_upload):
    """Run full deployment: generate catalog, create database, and upload."""
    warehouse_config = load_config(config)
    deployment_config = load_deployment_config(config)

    if not deployment_config:
        click.echo("Error: Deployment not configured. Add 'deployment:' section to warehouse.yml", err=True)
        sys.exit(1)

    click.echo(f"\n{'='*60}")
    click.echo("Full Deployment Pipeline")
    click.echo(f"{'='*60}\n")

    # Step 1: Generate catalog
    click.echo("Step 1/3: Generating catalog...")
    catalog_path = Path(warehouse_config.export_dir) / deployment_config.catalog_path
    catalog = generate_catalog(warehouse_config, deployment_config, catalog_path)
    click.echo(f"  ✓ Catalog generated: {len(catalog['tables'])} tables\n")

    # Step 2: Create remote database
    click.echo("Step 2/3: Creating remote views database...")
    db_path = Path(warehouse_config.export_dir) / deployment_config.database_path
    create_remote_database(warehouse_config, deployment_config, catalog, db_path)
    views_count = sum(1 for t in catalog['tables'].values() if 'export' in t)
    click.echo(f"  ✓ Database created: {views_count} views\n")

    if skip_upload:
        click.echo("Step 3/3: Upload skipped (--skip-upload)")
    else:
        # Step 3: Upload
        click.echo("Step 3/3: Uploading to R2...")
        # Call the upload command programmatically
        from click.testing import CliRunner
        runner = CliRunner()
        result = runner.invoke(deploy_upload, [
            '--config', config,
            '--data', '--catalog', '--database'
        ], input='y\n')

        if result.exit_code != 0:
            click.echo("  ✗ Upload failed", err=True)
            sys.exit(1)
        else:
            click.echo("  ✓ Upload complete")

    click.echo(f"\n{'='*60}")
    click.echo("Deployment complete!")
    click.echo(f"{'='*60}")


if __name__ == "__main__":
    warehouse()
