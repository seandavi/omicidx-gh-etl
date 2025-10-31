"""
Deployment utilities for publishing data warehouse to cloud storage.

This module provides:
- Catalog generation from schema.yml files
- Remote views database creation
- R2/S3 upload functionality
"""

import json
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Any
import duckdb
import yaml
from loguru import logger

from .config import DeploymentConfig, WarehouseConfig


def generate_catalog(
    warehouse_config: WarehouseConfig,
    deployment_config: DeploymentConfig,
    output_path: Optional[Path] = None
) -> Dict[str, Any]:
    """
    Generate a catalog.json file from schema.yml files and export metadata.

    The catalog contains:
    - Metadata about each table (name, description, columns)
    - File locations and sizes
    - Schema information
    - Last updated timestamps

    Args:
        warehouse_config: Warehouse configuration
        deployment_config: Deployment configuration
        output_path: Optional path to write catalog.json

    Returns:
        Dictionary containing catalog metadata
    """
    logger.info("Generating catalog from schema files...")

    catalog = {
        "version": "1.0",
        "generated_at": None,  # Will be set by DuckDB
        "public_url": deployment_config.public_url,
        "tables": {}
    }

    # Find all schema.yml files
    models_dir = Path(warehouse_config.models_dir)
    schema_files = list(models_dir.rglob("schema.yml"))

    logger.info(f"Found {len(schema_files)} schema files")

    for schema_file in schema_files:
        with open(schema_file) as f:
            schema_data = yaml.safe_load(f)

        if not schema_data or 'models' not in schema_data:
            continue

        # Determine layer from path
        relative_path = schema_file.parent.relative_to(models_dir)
        layer = str(relative_path) if str(relative_path) != '.' else 'default'

        for model in schema_data['models']:
            model_name = model['name']
            full_name = f"{layer}.{model_name}" if layer != 'default' else model_name

            # Extract model metadata
            table_info = {
                "name": model_name,
                "layer": layer,
                "full_name": full_name,
                "description": model.get('description', ''),
                "columns": [],
                "materialization": model.get('materialized', 'view'),
            }

            # Add column metadata
            for column in model.get('columns', []):
                table_info['columns'].append({
                    "name": column['name'],
                    "description": column.get('description', ''),
                    "data_type": column.get('data_type', 'unknown')
                })

            # Add export info if available
            if 'export' in model and model['export'].get('enabled'):
                export_path = model['export'].get('path')
                if export_path:
                    # Get file info if it exists
                    local_export_path = Path(warehouse_config.export_dir) / export_path
                    if local_export_path.exists():
                        file_size = local_export_path.stat().st_size
                        table_info['export'] = {
                            "path": export_path,
                            "format": model['export'].get('format', 'parquet'),
                            "compression": model['export'].get('compression', 'zstd'),
                            "file_size_bytes": file_size,
                            "file_size_mb": round(file_size / 1024 / 1024, 2)
                        }

                        # If we have a public URL, construct the full URL
                        if deployment_config.public_url:
                            file_url = f"{deployment_config.public_url}/{deployment_config.data_prefix}/{export_path}"
                            table_info['export']['url'] = file_url

            catalog['tables'][full_name] = table_info

    # Add generation timestamp
    with duckdb.connect(':memory:') as conn:
        catalog['generated_at'] = conn.execute("SELECT CURRENT_TIMESTAMP").fetchone()[0].isoformat()

    logger.info(f"Generated catalog with {len(catalog['tables'])} tables")

    # Write to file if requested
    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(catalog, f, indent=2)
        logger.info(f"Wrote catalog to: {output_path}")

    return catalog


def create_remote_database(
    warehouse_config: WarehouseConfig,
    deployment_config: DeploymentConfig,
    catalog: Dict[str, Any],
    output_path: Optional[Path] = None
) -> Path:
    """
    Create a lightweight DuckDB database with views pointing to remote parquet files.

    This creates a "data-less" database that users can query directly,
    with all data stored remotely in parquet files.

    Args:
        warehouse_config: Warehouse configuration
        deployment_config: Deployment configuration
        catalog: Catalog metadata (from generate_catalog)
        output_path: Optional path to write database file

    Returns:
        Path to created database file
    """
    logger.info("Creating remote views database...")

    if output_path is None:
        output_path = Path(warehouse_config.export_dir) / deployment_config.database_path

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Remove existing database
    if output_path.exists():
        output_path.unlink()
        logger.debug(f"Removed existing database: {output_path}")

    # Create new database
    with duckdb.connect(str(output_path)) as conn:
        logger.info(f"Creating database: {output_path}")

        # Create schemas (layers)
        schemas = set()
        for table_info in catalog['tables'].values():
            if table_info['layer'] != 'default':
                schemas.add(table_info['layer'])

        for schema in schemas:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            logger.debug(f"Created schema: {schema}")

        # Create views for exported tables
        views_created = 0
        for full_name, table_info in catalog['tables'].items():
            if 'export' not in table_info:
                logger.debug(f"Skipping {full_name}: not exported")
                continue

            # Determine the file URL
            if deployment_config.public_url:
                # Use public URL
                file_url = table_info['export']['url']
            else:
                # Use local path for testing
                file_url = str(Path(warehouse_config.export_dir) / table_info['export']['path'])

            # Create view
            view_sql = f"""
            CREATE VIEW {full_name} AS
            SELECT * FROM read_parquet('{file_url}')
            """

            try:
                conn.execute(view_sql)
                views_created += 1
                logger.debug(f"Created view: {full_name} -> {file_url}")
            except Exception as e:
                logger.error(f"Failed to create view {full_name}: {e}")

        # Store catalog metadata in the database
        conn.execute("CREATE TABLE _catalog AS SELECT $1 as catalog", [json.dumps(catalog)])
        logger.debug("Stored catalog metadata in database")

        logger.info(f"Created {views_created} remote views in database")

    return output_path


def upload_to_r2(
    local_path: Path,
    deployment_config: DeploymentConfig,
    remote_path: Optional[str] = None
) -> bool:
    """
    Upload a file to Cloudflare R2 using rclone or aws-cli.

    Args:
        local_path: Local file path to upload
        deployment_config: Deployment configuration
        remote_path: Remote path in bucket (if None, uses file name)

    Returns:
        True if upload succeeded, False otherwise
    """
    if not deployment_config.bucket_name:
        logger.error("Cannot upload: bucket_name not configured")
        return False

    local_path = Path(local_path)
    if not local_path.exists():
        logger.error(f"Cannot upload: file not found: {local_path}")
        return False

    if remote_path is None:
        remote_path = local_path.name

    logger.info(f"Uploading {local_path} to {deployment_config.bucket_name}/{remote_path}")

    # Try rclone first
    if _has_command('rclone'):
        return _upload_with_rclone(local_path, deployment_config, remote_path)

    # Fall back to aws-cli
    if _has_command('aws'):
        return _upload_with_aws_cli(local_path, deployment_config, remote_path)

    logger.error("Neither rclone nor aws-cli found. Please install one of them.")
    return False


def _has_command(command: str) -> bool:
    """Check if a command is available."""
    try:
        subprocess.run([command, '--version'], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def _upload_with_rclone(
    local_path: Path,
    deployment_config: DeploymentConfig,
    remote_path: str
) -> bool:
    """Upload using rclone."""
    logger.debug("Using rclone for upload")

    # rclone expects a remote name configured in rclone.conf
    # We'll assume the user has configured a remote named 'r2'
    remote_name = 'r2'

    cmd = [
        'rclone',
        'copyto',
        str(local_path),
        f"{remote_name}:{deployment_config.bucket_name}/{remote_path}",
        '--progress'
    ]

    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.info(f"Upload successful: {remote_path}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Upload failed: {e.stderr}")
        return False


def _upload_with_aws_cli(
    local_path: Path,
    deployment_config: DeploymentConfig,
    remote_path: str
) -> bool:
    """Upload using aws-cli."""
    logger.debug("Using aws-cli for upload")

    cmd = [
        'aws', 's3', 'cp',
        str(local_path),
        f"s3://{deployment_config.bucket_name}/{remote_path}"
    ]

    # Add endpoint URL if specified
    if deployment_config.endpoint_url:
        cmd.extend(['--endpoint-url', deployment_config.endpoint_url])

    # Set AWS credentials via environment if provided
    import os
    env = os.environ.copy()
    if deployment_config.access_key_id:
        env['AWS_ACCESS_KEY_ID'] = deployment_config.access_key_id
    if deployment_config.secret_access_key:
        env['AWS_SECRET_ACCESS_KEY'] = deployment_config.secret_access_key
    if deployment_config.region:
        env['AWS_DEFAULT_REGION'] = deployment_config.region

    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, env=env)
        logger.info(f"Upload successful: {remote_path}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Upload failed: {e.stderr}")
        return False


def upload_directory_to_r2(
    local_dir: Path,
    deployment_config: DeploymentConfig,
    remote_prefix: str = '',
    pattern: str = '**/*'
) -> Dict[str, bool]:
    """
    Upload all files in a directory to R2.

    Args:
        local_dir: Local directory to upload
        deployment_config: Deployment configuration
        remote_prefix: Prefix for remote paths
        pattern: Glob pattern for files to upload

    Returns:
        Dictionary mapping file paths to upload success status
    """
    local_dir = Path(local_dir)
    if not local_dir.is_dir():
        logger.error(f"Not a directory: {local_dir}")
        return {}

    files = list(local_dir.glob(pattern))
    files = [f for f in files if f.is_file()]

    logger.info(f"Uploading {len(files)} files from {local_dir}")

    results = {}
    for file_path in files:
        relative_path = file_path.relative_to(local_dir)
        remote_path = f"{remote_prefix}/{relative_path}" if remote_prefix else str(relative_path)
        success = upload_to_r2(file_path, deployment_config, remote_path)
        results[str(relative_path)] = success

    successful = sum(1 for success in results.values() if success)
    logger.info(f"Upload complete: {successful}/{len(files)} succeeded")

    return results
