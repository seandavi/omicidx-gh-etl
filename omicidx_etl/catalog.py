"""
Metadata cataloging module.

Scans all parquet files in the data directory and creates a catalog with
metadata about each file (row counts, columns, file sizes, etc.).
"""

from pathlib import Path
from typing import Optional
import click
from loguru import logger
from .db import duckdb_connection
from .config import settings


def build_file_catalog(
    data_dir: Path,
    output_path: Optional[Path] = None
) -> Path:
    """
    Build a catalog of all parquet files in the data directory.

    Args:
        data_dir: Root directory to scan for parquet files
        output_path: Where to save catalog.parquet (default: data_dir/catalog.parquet)

    Returns:
        Path to the created catalog file
    """
    if output_path is None:
        output_path = data_dir / "catalog.parquet"

    logger.info(f"Building catalog from {data_dir}")

    with duckdb_connection() as con:
        # Scan all parquet files recursively
        pattern = str(data_dir / "**/*.parquet")

        logger.info(f"Scanning pattern: {pattern}")

        # Use parquet_metadata to get detailed info about each file
        try:
            con.execute(f"""
                CREATE OR REPLACE TABLE file_catalog AS
                SELECT
                    file_name,
                    row_group_id,
                    row_group_num_rows,
                    row_group_num_columns,
                    row_group_bytes as row_group_compressed_size,
                    total_byte_size as total_uncompressed_size,
                    num_columns,
                    num_rows,
                    num_row_groups,
                    format_version
                FROM parquet_metadata('{pattern}')
                WHERE file_name NOT LIKE '%catalog.parquet%'  -- Exclude catalog file itself
            """)

            # Get summary statistics
            summary = con.execute("""
                SELECT
                    COUNT(DISTINCT file_name) as total_files,
                    SUM(num_rows) as total_rows,
                    SUM(total_uncompressed_size) as total_size_bytes,
                    COUNT(DISTINCT row_group_id) as total_row_groups
                FROM file_catalog
            """).fetchone()

            total_files, total_rows, total_size, total_row_groups = summary

            logger.info(f"Found {total_files:,} parquet files")
            logger.info(f"Total rows: {total_rows:,}")
            logger.info(f"Total size: {total_size / 1024**3:.2f} GB")
            logger.info(f"Total row groups: {total_row_groups:,}")

            # Export catalog
            con.execute(f"""
                COPY file_catalog TO '{output_path}'
                (FORMAT PARQUET, COMPRESSION ZSTD)
            """)

            logger.info(f"✓ Catalog saved to {output_path}")

        except Exception as e:
            logger.error(f"Failed to build catalog: {e}")
            raise

    return output_path


def show_catalog_summary(catalog_path: Path):
    """
    Display a summary of the catalog.

    Args:
        catalog_path: Path to catalog.parquet file
    """
    if not catalog_path.exists():
        logger.error(f"Catalog file not found: {catalog_path}")
        return

    with duckdb_connection() as con:
        # Overall summary
        logger.info("\n" + "=" * 60)
        logger.info("Catalog Summary")
        logger.info("=" * 60)

        summary = con.execute(f"""
            SELECT
                COUNT(DISTINCT file_name) as total_files,
                SUM(num_rows) as total_rows,
                SUM(total_uncompressed_size) as total_size_bytes,
                COUNT(DISTINCT row_group_id) as total_row_groups
            FROM read_parquet('{catalog_path}')
        """).fetchone()

        total_files, total_rows, total_size, total_row_groups = summary

        logger.info(f"Total files: {total_files:,}")
        logger.info(f"Total rows: {total_rows:,}")
        logger.info(f"Total size: {total_size / 1024**3:.2f} GB")
        logger.info(f"Total row groups: {total_row_groups:,}")

        # By directory
        logger.info("\n" + "=" * 60)
        logger.info("Files by Directory")
        logger.info("=" * 60)

        by_dir = con.execute(f"""
            SELECT
                regexp_extract(file_name, '.*/([^/]+)/[^/]+$', 1) as directory,
                COUNT(DISTINCT file_name) as file_count,
                SUM(num_rows) as row_count,
                SUM(total_uncompressed_size) / (1024*1024*1024) as size_gb
            FROM read_parquet('{catalog_path}')
            GROUP BY directory
            ORDER BY row_count DESC
        """).fetchall()

        for directory, file_count, row_count, size_gb in by_dir:
            if directory:
                logger.info(
                    f"{directory:20s} | {file_count:5,} files | "
                    f"{row_count:12,} rows | {size_gb:8.2f} GB"
                )

        logger.info("=" * 60)


# CLI Interface

@click.group()
def catalog():
    """Metadata cataloging commands."""
    pass


@catalog.command()
@click.option(
    '--data-dir',
    type=click.Path(path_type=Path),
    default=None,
    help=f'Root data directory to scan (default: {settings.PUBLISH_DIRECTORY})'
)
@click.option(
    '--output',
    type=click.Path(path_type=Path),
    default=None,
    help='Output path for catalog.parquet (default: data-dir/catalog.parquet)'
)
def build(data_dir: Optional[Path], output: Optional[Path]):
    """
    Build a catalog of all parquet files.

    Scans the data directory recursively and creates a parquet file with
    metadata about every parquet file found (row counts, sizes, etc.).

    Examples:

        # Use default data directory from config
        oidx catalog build

        # Specify custom directory
        oidx catalog build --data-dir /data/omicidx

        # Custom output location
        oidx catalog build --output /tmp/my_catalog.parquet
    """
    if data_dir is None:
        data_dir = Path(settings.PUBLISH_DIRECTORY)

    try:
        catalog_path = build_file_catalog(data_dir, output)
        logger.info(f"\n✓ Catalog created successfully: {catalog_path}")

    except Exception as e:
        logger.error(f"Failed to build catalog: {e}")
        raise click.Abort()


@catalog.command()
@click.option(
    '--catalog-path',
    type=click.Path(path_type=Path),
    default=None,
    help='Path to catalog.parquet (default: {PUBLISH_DIRECTORY}/catalog.parquet)'
)
def show(catalog_path: Optional[Path]):
    """
    Show a summary of the catalog.

    Displays statistics about parquet files in the catalog including
    counts, sizes, and breakdown by directory.

    Examples:

        # Show default catalog
        oidx catalog show

        # Show specific catalog file
        oidx catalog show --catalog-path /data/omicidx/catalog.parquet
    """
    if catalog_path is None:
        catalog_path = Path(settings.PUBLISH_DIRECTORY) / "catalog.parquet"

    if not catalog_path.exists():
        logger.error(f"Catalog not found: {catalog_path}")
        logger.info("Run 'oidx catalog build' first to create a catalog")
        raise click.Abort()

    show_catalog_summary(catalog_path)


@catalog.command()
@click.option(
    '--data-dir',
    type=click.Path(path_type=Path),
    default=None,
    help=f'Root data directory (default: {settings.PUBLISH_DIRECTORY})'
)
def refresh(data_dir: Optional[Path]):
    """
    Refresh the catalog (rebuild and show summary).

    Convenience command that rebuilds the catalog and displays the summary.
    """
    if data_dir is None:
        data_dir = Path(settings.PUBLISH_DIRECTORY)

    try:
        catalog_path = build_file_catalog(data_dir)
        show_catalog_summary(catalog_path)

    except Exception as e:
        logger.error(f"Failed to refresh catalog: {e}")
        raise click.Abort()


if __name__ == "__main__":
    catalog()
