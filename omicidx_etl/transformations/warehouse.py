"""
DuckDB warehouse orchestrator with dbt-compatible structure.

This module provides a lightweight alternative to dbt that:
- Uses SQL files in a dbt-compatible directory structure
- Tracks metadata and lineage in DuckDB
- Maintains compatibility for future dbt migration
- Keeps DuckDB-specific optimizations (direct parquet access, etc.)
"""

import duckdb
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from pathlib import Path
from loguru import logger
import yaml
import json
from datetime import datetime
import hashlib


@dataclass
class WarehouseConfig:
    """Configuration for the data warehouse."""
    db_path: str = 'omicidx_warehouse.duckdb'
    models_dir: str = 'omicidx_etl/transformations/models'
    export_dir: str = 'exports'
    threads: int = 16
    memory_limit: str = '32GB'
    temp_directory: Optional[str] = None


@dataclass
class ExportConfig:
    """Configuration for model exports."""
    enabled: bool = False
    path: Optional[str] = None  # Relative path in export directory
    format: str = 'parquet'
    compression: str = 'zstd'
    partition_by: Optional[List[str]] = None
    row_group_size: int = 100000


@dataclass
class ModelConfig:
    """Configuration for a single model (table/view)."""
    name: str
    layer: str  # 'raw', 'staging', or 'mart'
    sql_path: Path
    materialization: str = 'view'  # 'view', 'table', 'export_table', 'export_view'
    depends_on: List[str] = field(default_factory=list)
    description: Optional[str] = None
    columns: Dict[str, str] = field(default_factory=dict)  # column_name -> description
    tags: List[str] = field(default_factory=list)
    export: ExportConfig = field(default_factory=ExportConfig)

    @property
    def full_name(self) -> str:
        """Fully qualified name: layer.name"""
        return f"{self.layer}.{self.name}"

    @property
    def should_export(self) -> bool:
        """Check if this model should be exported."""
        return self.export.enabled or self.materialization in ('export_table', 'export_view')


class WarehouseConnection:
    """Managed DuckDB connection with warehouse-specific settings."""

    def __init__(self, config: WarehouseConfig):
        self.config = config
        self.conn: Optional[duckdb.DuckDBPyConnection] = None

    def __enter__(self) -> duckdb.DuckDBPyConnection:
        """Open connection and configure warehouse."""
        logger.info(f"Opening warehouse: {self.config.db_path}")
        self.conn = duckdb.connect(self.config.db_path)

        # Configure DuckDB
        self.conn.execute(f"PRAGMA threads={self.config.threads};")
        self.conn.execute(f"PRAGMA memory_limit='{self.config.memory_limit}';")

        if self.config.temp_directory:
            self.conn.execute(f"PRAGMA temp_directory='{self.config.temp_directory}';")
            logger.debug(f"Set temp_directory to {self.config.temp_directory}")

        # Initialize warehouse schemas
        self._init_schemas()

        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close connection."""
        if self.conn:
            self.conn.close()
            logger.info("Warehouse connection closed")

    def _init_schemas(self):
        """Create warehouse schemas if they don't exist."""
        schemas = ['raw', 'staging', 'mart', 'meta']

        for schema in schemas:
            self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            logger.debug(f"Initialized schema: {schema}")

        # Create metadata tracking tables
        self._init_metadata_tables()

    def _init_metadata_tables(self):
        """Create metadata tracking tables in the meta schema."""

        # Model runs tracking
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS meta.model_runs (
                run_id VARCHAR PRIMARY KEY,
                model_name VARCHAR NOT NULL,
                layer VARCHAR NOT NULL,
                started_at TIMESTAMP NOT NULL,
                completed_at TIMESTAMP,
                status VARCHAR NOT NULL,  -- 'running', 'success', 'error'
                error_message TEXT,
                rows_affected BIGINT,
                execution_time_seconds DOUBLE,
                sql_hash VARCHAR,
                metadata JSON
            );
        """)

        # Model lineage tracking
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS meta.model_lineage (
                model_name VARCHAR NOT NULL,
                depends_on_model VARCHAR NOT NULL,
                layer VARCHAR NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (model_name, depends_on_model)
            );
        """)

        # Model documentation
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS meta.model_docs (
                model_name VARCHAR PRIMARY KEY,
                layer VARCHAR NOT NULL,
                description TEXT,
                columns JSON,
                tags JSON,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        logger.debug("Initialized metadata tables")


def discover_models(models_dir: str) -> List[ModelConfig]:
    """
    Discover all SQL models in the models directory.

    Args:
        models_dir: Path to models directory

    Returns:
        List of ModelConfig objects
    """
    models_path = Path(models_dir)
    models = []

    # Scan each layer
    for layer in ['raw', 'staging', 'mart']:
        layer_path = models_path / layer

        if not layer_path.exists():
            continue

        # Find all SQL files
        for sql_file in layer_path.glob('*.sql'):
            model_name = sql_file.stem

            # Parse schema.yml for metadata if it exists
            schema_file = layer_path / 'schema.yml'
            metadata = _parse_schema_yml(schema_file, model_name) if schema_file.exists() else {}

            model = ModelConfig(
                name=model_name,
                layer=layer,
                sql_path=sql_file,
                description=metadata.get('description'),
                columns=metadata.get('columns', {}),
                tags=metadata.get('tags', []),
                depends_on=metadata.get('depends_on', []),
                materialization=metadata.get('materialization', 'view'),
                export=metadata.get('export', ExportConfig())
            )

            models.append(model)
            logger.debug(f"Discovered model: {model.full_name}")

    return models


def _parse_schema_yml(schema_path: Path, model_name: str) -> Dict[str, Any]:
    """Parse schema.yml file for model metadata."""
    try:
        with open(schema_path) as f:
            schema_data = yaml.safe_load(f)

        # Find this model's config in the schema file
        for model in schema_data.get('models', []):
            if model.get('name') == model_name:
                # Extract metadata
                metadata = {
                    'description': model.get('description'),
                    'tags': model.get('tags', []),
                    'depends_on': model.get('depends_on', []),
                    'materialization': model.get('materialized', 'view'),
                    'columns': {}
                }

                # Extract column descriptions
                for col in model.get('columns', []):
                    metadata['columns'][col['name']] = col.get('description', '')

                # Extract export configuration
                export_config = model.get('export', {})
                if export_config:
                    metadata['export'] = ExportConfig(
                        enabled=export_config.get('enabled', False),
                        path=export_config.get('path'),
                        format=export_config.get('format', 'parquet'),
                        compression=export_config.get('compression', 'zstd'),
                        partition_by=export_config.get('partition_by'),
                        row_group_size=export_config.get('row_group_size', 100000)
                    )

                return metadata

        return {}

    except Exception as e:
        logger.warning(f"Failed to parse schema.yml: {e}")
        return {}


def topological_sort(models: List[ModelConfig]) -> List[ModelConfig]:
    """
    Sort models in topological order based on dependencies.

    Args:
        models: List of model configurations

    Returns:
        List of models sorted so dependencies come before dependents

    Raises:
        ValueError: If circular dependency detected
    """
    graph = {model.full_name: model for model in models}
    dependencies = {model.full_name: set(model.depends_on) for model in models}

    visited = set()
    temp_visited = set()
    result = []

    def visit(name: str):
        if name in temp_visited:
            raise ValueError(f"Circular dependency detected involving: {name}")
        if name in visited:
            return

        temp_visited.add(name)

        for dep in dependencies.get(name, []):
            if dep in graph:
                visit(dep)
            else:
                logger.warning(f"Model {name} depends on undefined model: {dep}")

        temp_visited.remove(name)
        visited.add(name)
        if name in graph:
            result.append(graph[name])

    for model in models:
        if model.full_name not in visited:
            visit(model.full_name)

    return result


def _export_model(
    conn: duckdb.DuckDBPyConnection,
    model: ModelConfig,
    config: WarehouseConfig
) -> Optional[str]:
    """
    Export a model to parquet file.

    Args:
        conn: DuckDB connection
        model: Model configuration
        config: Warehouse configuration

    Returns:
        Export path if successful, None otherwise
    """
    if not model.should_export:
        return None

    # Determine export path
    if model.export.path:
        export_path = Path(config.export_dir) / model.export.path
    else:
        # Default: layer/model_name.parquet
        export_path = Path(config.export_dir) / model.layer / f"{model.name}.parquet"

    # Create export directory
    export_path.parent.mkdir(parents=True, exist_ok=True)

    # Build COPY statement
    partition_clause = ""
    if model.export.partition_by:
        partition_clause = f", PARTITION_BY ({', '.join(model.export.partition_by)})"

    copy_sql = f"""
    COPY {model.layer}.{model.name}
    TO '{export_path}' (
        FORMAT {model.export.format},
        COMPRESSION {model.export.compression},
        ROW_GROUP_SIZE {model.export.row_group_size}
        {partition_clause}
    )
    """

    logger.info(f"Exporting {model.full_name} to {export_path}")
    conn.execute(copy_sql)
    logger.success(f"Exported to: {export_path}")

    return str(export_path)


def execute_model(
    conn: duckdb.DuckDBPyConnection,
    model: ModelConfig,
    config: WarehouseConfig,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Execute a single model.

    Args:
        conn: DuckDB connection
        model: Model configuration
        config: Warehouse configuration
        dry_run: If True, only parse SQL without executing

    Returns:
        Dictionary with execution metadata
    """
    run_id = f"{model.full_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger.info(f"Executing model: {model.full_name} (materialization={model.materialization})")

    try:
        # Read SQL file
        with open(model.sql_path) as f:
            sql = f.read()

        sql_hash = hashlib.md5(sql.encode()).hexdigest()

        if dry_run:
            logger.info(f"[DRY RUN] Would execute: {model.full_name}")
            return {
                'run_id': run_id,
                'model_name': model.full_name,
                'status': 'dry_run',
                'sql_hash': sql_hash
            }

        # Record start
        started_at = datetime.now()
        conn.execute("""
            INSERT INTO meta.model_runs (run_id, model_name, layer, started_at, status, sql_hash)
            VALUES (?, ?, ?, ?, 'running', ?)
        """, [run_id, model.name, model.layer, started_at, sql_hash])

        # Execute based on materialization type
        export_path = None

        if model.materialization == 'view':
            full_sql = f"CREATE OR REPLACE VIEW {model.layer}.{model.name} AS\n{sql}"
            conn.execute(full_sql)
            rows_affected = None

        elif model.materialization == 'table':
            full_sql = f"CREATE OR REPLACE TABLE {model.layer}.{model.name} AS\n{sql}"
            result = conn.execute(full_sql)
            rows_affected = result.fetchone()[0] if result else None

        elif model.materialization == 'export_view':
            # Create view AND export it
            full_sql = f"CREATE OR REPLACE VIEW {model.layer}.{model.name} AS\n{sql}"
            conn.execute(full_sql)
            rows_affected = None
            export_path = _export_model(conn, model, config)

        elif model.materialization == 'export_table':
            # Create table AND export it
            full_sql = f"CREATE OR REPLACE TABLE {model.layer}.{model.name} AS\n{sql}"
            result = conn.execute(full_sql)
            rows_affected = result.fetchone()[0] if result else None
            export_path = _export_model(conn, model, config)

        elif model.materialization == 'external_table':
            # For external tables, SQL should be a COPY statement or similar
            conn.execute(sql)
            rows_affected = None

        else:
            raise ValueError(f"Unsupported materialization: {model.materialization}")

        # Record success
        completed_at = datetime.now()
        execution_time = (completed_at - started_at).total_seconds()

        conn.execute("""
            UPDATE meta.model_runs
            SET completed_at = ?,
                status = 'success',
                rows_affected = ?,
                execution_time_seconds = ?
            WHERE run_id = ?
        """, [completed_at, rows_affected, execution_time, run_id])

        if export_path:
            logger.success(f"Model {model.full_name} completed in {execution_time:.2f}s (exported to {export_path})")
        else:
            logger.success(f"Model {model.full_name} completed in {execution_time:.2f}s")

        return {
            'run_id': run_id,
            'model_name': model.full_name,
            'status': 'success',
            'execution_time_seconds': execution_time,
            'rows_affected': rows_affected,
            'export_path': export_path
        }

    except Exception as e:
        logger.error(f"Failed to execute model {model.full_name}: {e}")

        # Record error
        conn.execute("""
            UPDATE meta.model_runs
            SET completed_at = CURRENT_TIMESTAMP,
                status = 'error',
                error_message = ?
            WHERE run_id = ?
        """, [str(e), run_id])

        return {
            'run_id': run_id,
            'model_name': model.full_name,
            'status': 'error',
            'error': str(e)
        }


def update_metadata(conn: duckdb.DuckDBPyConnection, models: List[ModelConfig]):
    """
    Update metadata tables with current model configurations.

    Args:
        conn: DuckDB connection
        models: List of model configurations
    """
    logger.info("Updating metadata tables")

    # Update lineage
    conn.execute("DELETE FROM meta.model_lineage")
    for model in models:
        for dep in model.depends_on:
            conn.execute("""
                INSERT INTO meta.model_lineage (model_name, depends_on_model, layer)
                VALUES (?, ?, ?)
            """, [model.name, dep.split('.')[-1], model.layer])

    # Update documentation
    for model in models:
        conn.execute("""
            INSERT OR REPLACE INTO meta.model_docs
            (model_name, layer, description, columns, tags, updated_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, [
            model.name,
            model.layer,
            model.description,
            json.dumps(model.columns) if model.columns else None,
            json.dumps(model.tags) if model.tags else None
        ])

    logger.info(f"Updated metadata for {len(models)} models")


def run_warehouse(
    config: WarehouseConfig,
    models: Optional[List[str]] = None,
    dry_run: bool = False,
    fail_fast: bool = True
) -> List[Dict[str, Any]]:
    """
    Run warehouse transformations.

    Args:
        config: Warehouse configuration
        models: List of specific models to run (None = all models)
        dry_run: If True, only show what would be executed
        fail_fast: If True, stop on first error

    Returns:
        List of execution results
    """
    logger.info("Starting warehouse run")

    # Discover models
    all_models = discover_models(config.models_dir)
    logger.info(f"Discovered {len(all_models)} models")

    # Filter models if specific ones requested
    if models:
        model_set = set(models)
        all_models = [m for m in all_models if m.name in model_set or m.full_name in model_set]
        logger.info(f"Filtered to {len(all_models)} models")

    # Sort by dependencies
    sorted_models = topological_sort(all_models)
    logger.info(f"Execution order: {[m.full_name for m in sorted_models]}")

    # Execute models
    results = []

    with WarehouseConnection(config) as conn:
        # Update metadata
        if not dry_run:
            update_metadata(conn, sorted_models)

        # Run each model
        for model in sorted_models:
            result = execute_model(conn, model, config, dry_run=dry_run)
            results.append(result)

            # Stop on error if fail_fast
            if fail_fast and result['status'] == 'error':
                logger.error(f"Stopping due to error in: {model.full_name}")
                break

    # Summary
    success_count = sum(1 for r in results if r['status'] == 'success')
    error_count = sum(1 for r in results if r['status'] == 'error')
    logger.info(f"Warehouse run complete: {success_count} succeeded, {error_count} failed")

    return results


if __name__ == "__main__":
    import sys

    # Simple CLI
    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "run":
            config = WarehouseConfig()
            results = run_warehouse(config)

            # Exit with error if any failed
            if any(r['status'] == 'error' for r in results):
                sys.exit(1)

        elif command == "dry-run":
            config = WarehouseConfig()
            results = run_warehouse(config, dry_run=True)

        else:
            print("Usage:")
            print("  python warehouse.py run       # Run all models")
            print("  python warehouse.py dry-run   # Show execution plan")
            sys.exit(1)
    else:
        print("Usage:")
        print("  python warehouse.py run       # Run all models")
        print("  python warehouse.py dry-run   # Show execution plan")
        sys.exit(1)
