import duckdb
from dataclasses import dataclass, field
from typing import List, Optional
from loguru import logger
from pathlib import Path


@dataclass
class TransformationConfig:
    """Global configuration for transformations."""
    root_path: str
    transformed_dir: str = "transformed"
    threads: int = 16
    compression: str = "zstd"
    union_by_name: bool = True


@dataclass
class TableConfig:
    """Configuration for a single table transformation."""
    name: str
    source_pattern: str
    source_format: str  # 'ndjson' or 'parquet'
    output_name: Optional[str] = None
    depends_on: List[str] = field(default_factory=list)
    sql_template: Optional[str] = None  # Path to custom SQL file

    def __post_init__(self):
        # Convention: output name defaults to {name}.parquet
        if self.output_name is None:
            self.output_name = f"{self.name}.parquet"


# Define all raw tables (no dependencies)
RAW_TABLES = [
    TableConfig(
        name="gsm",
        source_pattern="geo/gsm*.ndjson.gz",
        source_format="ndjson",
    ),
    TableConfig(
        name="gse",
        source_pattern="geo/gse*.ndjson.gz",
        source_format="ndjson",
    ),
    TableConfig(
        name="gpl",
        source_pattern="geo/gpl*.ndjson.gz",
        source_format="ndjson",
    ),
    TableConfig(
        name="sra_studies",
        source_pattern="sra/*study*.parquet",
        source_format="parquet",
    ),
    TableConfig(
        name="sra_experiments",
        source_pattern="sra/*experiment*.parquet",
        source_format="parquet",
    ),
    TableConfig(
        name="sra_samples",
        source_pattern="sra/*sample*.parquet",
        source_format="parquet",
    ),
    TableConfig(
        name="sra_runs",
        source_pattern="sra/*run*.parquet",
        source_format="parquet",
    ),
    TableConfig(
        name="ncbi_biosamples",
        source_pattern="biosample/biosample*.parquet",
        source_format="parquet",
    ),
    TableConfig(
        name="ncbi_bioprojects",
        source_pattern="biosample/bioproject*.parquet",
        source_format="parquet",
    ),
]


def create_connection(db_path: str = ':memory:', threads: int = 16) -> duckdb.DuckDBPyConnection:
    """
    Create and return a DuckDB connection.

    Args:
        db_path: Path to the DuckDB database file
        threads: Number of threads for DuckDB to use
    """
    logger.info(f"Creating DuckDB connection: {db_path}")
    conn = duckdb.connect(db_path)
    conn.execute(f"PRAGMA threads={threads};")
    logger.debug(f"Set threads to {threads}")
    return conn


def build_dependency_graph(tables: List[TableConfig]) -> dict[str, List[str]]:
    """
    Build a dependency graph from table configurations.

    Returns:
        Dictionary mapping table name to list of tables it depends on
    """
    graph = {}
    for table in tables:
        graph[table.name] = table.depends_on
    return graph


def topological_sort(tables: List[TableConfig]) -> List[TableConfig]:
    """
    Sort tables in topological order based on dependencies.

    Args:
        tables: List of table configurations

    Returns:
        List of tables sorted so dependencies come before dependents

    Raises:
        ValueError: If circular dependency detected
    """
    # Build graph
    graph = {table.name: table for table in tables}
    dependencies = {table.name: set(table.depends_on) for table in tables}

    # Track visited nodes for cycle detection
    visited = set()
    temp_visited = set()
    result = []

    def visit(name: str):
        if name in temp_visited:
            raise ValueError(f"Circular dependency detected involving table: {name}")
        if name in visited:
            return

        temp_visited.add(name)

        for dep in dependencies.get(name, []):
            if dep not in graph:
                raise ValueError(f"Table {name} depends on undefined table: {dep}")
            visit(dep)

        temp_visited.remove(name)
        visited.add(name)
        result.append(graph[name])

    for table in tables:
        if table.name not in visited:
            visit(table.name)

    return result


def generate_copy_sql(table: TableConfig, config: TransformationConfig) -> str:
    """
    Generate SQL COPY statement for a table.

    Args:
        table: Table configuration
        config: Global transformation configuration

    Returns:
        SQL string
    """
    source_path = f"{config.root_path}/{table.source_pattern}"
    output_path = f"{config.root_path}/{config.transformed_dir}/{table.output_name}"

    # Choose reader function based on format
    if table.source_format == "ndjson":
        reader = f"read_ndjson_auto('{source_path}', union_by_name := {str(config.union_by_name).lower()})"
    elif table.source_format == "parquet":
        reader = f"read_parquet('{source_path}', union_by_name := {str(config.union_by_name).lower()})"
    else:
        raise ValueError(f"Unsupported source format: {table.source_format}")

    sql = f"""
    COPY (
        SELECT * FROM {reader}
    )
    TO '{output_path}' (
        FORMAT parquet,
        COMPRESSION {config.compression}
    );
    """

    return sql


def execute_table_transformation(
    conn: duckdb.DuckDBPyConnection,
    table: TableConfig,
    config: TransformationConfig
) -> dict:
    """
    Execute transformation for a single table.

    Args:
        conn: DuckDB connection
        table: Table configuration
        config: Global transformation configuration

    Returns:
        Dictionary with execution metadata (name, status, error if any)
    """
    logger.info(f"Transforming table: {table.name}")

    try:
        # Use custom SQL template if provided, otherwise generate COPY statement
        if table.sql_template:
            sql_path = Path(table.sql_template)
            logger.debug(f"Loading SQL template: {sql_path}")
            with open(sql_path, 'r') as f:
                sql = f.read()
        else:
            sql = generate_copy_sql(table, config)

        logger.debug(f"Executing SQL for {table.name}")
        conn.execute(sql)

        output_path = f"{config.root_path}/{config.transformed_dir}/{table.output_name}"
        logger.success(f"Successfully created table: {table.name} -> {output_path}")

        return {
            "name": table.name,
            "status": "success",
            "output": output_path
        }

    except Exception as e:
        logger.error(f"Failed to transform table {table.name}: {e}")
        return {
            "name": table.name,
            "status": "error",
            "error": str(e)
        }


def run_transformations(
    tables: List[TableConfig],
    config: TransformationConfig,
    db_path: str = ':memory:'
) -> List[dict]:
    """
    Run all transformations in dependency order.

    Args:
        tables: List of table configurations
        config: Global transformation configuration
        db_path: Path to DuckDB database

    Returns:
        List of execution results for each table
    """
    logger.info(f"Starting transformations for {len(tables)} tables")

    # Create output directory
    output_dir = Path(config.root_path) / config.transformed_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Output directory: {output_dir}")

    # Sort tables by dependencies
    sorted_tables = topological_sort(tables)
    logger.info(f"Execution order: {[t.name for t in sorted_tables]}")

    # Create connection
    conn = create_connection(db_path, config.threads)

    # Execute transformations
    results = []
    for table in sorted_tables:
        result = execute_table_transformation(conn, table, config)
        results.append(result)

        # Stop on error (fail fast)
        if result["status"] == "error":
            logger.error(f"Stopping execution due to error in table: {table.name}")
            break

    conn.close()
    logger.info("Transformation execution complete")

    # Summary
    success_count = sum(1 for r in results if r["status"] == "success")
    error_count = sum(1 for r in results if r["status"] == "error")
    logger.info(f"Summary: {success_count} succeeded, {error_count} failed")

    return results


def create_remote_views(
    tables: List[TableConfig],
    https_root: str,
    db_path: str = 'omicidx.duckdb',
    threads: int = 16
) -> List[dict]:
    """
    Create views in a DuckDB database pointing to remote parquet files.

    Args:
        tables: List of table configurations
        https_root: Root URL for remote files (e.g., 'https://store.cancerdatasci.org/omicidx/raw')
        db_path: Path to DuckDB database file
        threads: Number of threads for DuckDB to use

    Returns:
        List of results for each view created

    Example:
        >>> create_remote_views(
        ...     RAW_TABLES,
        ...     'https://store.cancerdatasci.org/omicidx/raw',
        ...     'omicidx.duckdb'
        ... )
    """
    logger.info(f"Creating remote views in database: {db_path}")
    logger.info(f"Remote root: {https_root}")

    # Remove trailing slash from https_root if present
    https_root = https_root.rstrip('/')

    # Create connection
    conn = create_connection(db_path, threads)

    results = []

    for table in tables:
        logger.info(f"Creating view for table: {table.name}")

        try:
            # Construct remote URL
            remote_url = f"{https_root}/{table.output_name}"

            # Create view
            view_name = table.name
            sql = f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT * FROM read_parquet('{remote_url}');
            """

            logger.debug(f"Executing: CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{remote_url}')")
            conn.execute(sql)

            logger.success(f"Successfully created view: {view_name} -> {remote_url}")

            results.append({
                "name": table.name,
                "view_name": view_name,
                "remote_url": remote_url,
                "status": "success"
            })

        except Exception as e:
            logger.error(f"Failed to create view for {table.name}: {e}")
            results.append({
                "name": table.name,
                "status": "error",
                "error": str(e)
            })

    conn.close()
    logger.info("View creation complete")

    # Summary
    success_count = sum(1 for r in results if r["status"] == "success")
    error_count = sum(1 for r in results if r["status"] == "error")
    logger.info(f"Summary: {success_count} views created, {error_count} failed")

    return results


if __name__ == "__main__":
    import sys

    # Example 1: Run transformations locally
    if len(sys.argv) > 1 and sys.argv[1] == "transform":
        config = TransformationConfig(root_path='/data/davsean/omicidx_root')
        results = run_transformations(RAW_TABLES, config)

        # Exit with error code if any transformations failed
        if any(r["status"] == "error" for r in results):
            exit(1)

    # Example 2: Create remote views database
    elif len(sys.argv) > 1 and sys.argv[1] == "create-views":
        results = create_remote_views(
            RAW_TABLES,
            'https://store.cancerdatasci.org/omicidx/raw',
            '/data/davsean/omicidx_root/omicidx.duckdb'
        )

        # Exit with error code if any views failed
        if any(r["status"] == "error" for r in results):
            exit(1)

    else:
        print("Usage:")
        print("  python create_raw.py transform      # Run local transformations")
        print("  python create_raw.py create-views   # Create remote views database")
        exit(1)
