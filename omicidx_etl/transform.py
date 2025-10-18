"""
Data transformation module using DuckDB.

This module provides transformation capabilities to consolidate and process
extracted data. Transformations can be added as functions and called via CLI.
"""

from pathlib import Path
from typing import Optional
import click
from loguru import logger
from .db import duckdb_connection
from .config import settings


def consolidate_sra_entities(
    extract_dir: Path,
    output_dir: Path
) -> dict[str, int]:
    """
    Consolidate chunked SRA parquet files into single files per entity type.

    SRA extraction creates chunked files like:
    - 20251001_Full-experiment-0001.parquet
    - 20251001_Full-experiment-0002.parquet
    - etc.

    This consolidates them into:
    - experiments.parquet
    - runs.parquet
    - samples.parquet
    - studies.parquet

    Args:
        extract_dir: Directory containing extracted SRA parquet files
        output_dir: Directory where consolidated files will be written

    Returns:
        Dictionary with entity type and row counts
    """
    sra_dir = extract_dir / "sra"

    if not sra_dir.exists():
        raise ValueError(f"SRA directory not found: {sra_dir}")

    logger.info(f"Consolidating SRA data from {sra_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    results = {}

    with duckdb_connection() as con:
        # Define entity types to consolidate
        entities = ["experiment", "run", "sample", "study"]

        for entity in entities:
            logger.info(f"Consolidating {entity} files...")

            # Pattern to match all chunks for this entity
            pattern = str(sra_dir / f"*{entity}*.parquet")

            # Check if files exist
            file_count = con.execute(f"""
                SELECT COUNT(*) as cnt
                FROM glob('{pattern}')
            """).fetchone()[0]

            if file_count == 0:
                logger.warning(f"No {entity} files found matching pattern: {pattern}")
                results[entity] = 0
                continue

            logger.info(f"Found {file_count} {entity} file(s)")

            # Consolidate all chunks into single table
            output_path = output_dir / f"{entity}s.parquet"

            con.execute(f"""
                COPY (
                    SELECT * FROM read_parquet('{pattern}')
                ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)

            # Get row count
            row_count = con.execute(f"""
                SELECT COUNT(*) as cnt
                FROM read_parquet('{output_path}')
            """).fetchone()[0]

            results[entity] = row_count
            logger.info(f"✓ Created {output_path} with {row_count:,} rows")

    return results


def run_custom_transformations(
    extract_dir: Path,
    output_dir: Path
) -> dict[str, any]:
    """
    Run custom DuckDB transformations on extracted data.

    Add your own transformation queries here. Examples:
    - Aggregations across datasets
    - Joins between SRA, biosample, GEO
    - Summary statistics
    - Data quality checks

    Args:
        extract_dir: Directory containing all extracted data
        output_dir: Directory where transformed data will be written

    Returns:
        Dictionary with transformation results
    """
    logger.info("Running custom transformations")
    output_dir.mkdir(parents=True, exist_ok=True)

    results = {}

    with duckdb_connection() as con:
        # Example: Create a study summary with counts
        logger.info("Creating study summary...")

        try:
            con.execute(f"""
                CREATE OR REPLACE TABLE study_summary AS
                SELECT
                    accession,
                    COUNT(*) as record_count
                FROM read_parquet('{extract_dir}/sra/*study*.parquet')
                GROUP BY accession
                ORDER BY record_count DESC
            """)

            # Export to parquet
            study_summary_path = output_dir / "study_summary.parquet"
            con.execute(f"""
                COPY study_summary TO '{study_summary_path}'
                (FORMAT PARQUET, COMPRESSION ZSTD)
            """)

            row_count = con.execute("SELECT COUNT(*) FROM study_summary").fetchone()[0]
            results["study_summary"] = row_count
            logger.info(f"✓ Created study_summary.parquet with {row_count:,} studies")

        except Exception as e:
            logger.warning(f"Study summary transformation failed: {e}")
            results["study_summary"] = None

        # Add more transformations here as needed:
        # - Join SRA with biosample data
        # - Aggregate by organism
        # - Calculate quality metrics
        # etc.

    return results


def run_all_transformations(
    extract_dir: Optional[Path] = None,
    output_dir: Optional[Path] = None
) -> dict[str, any]:
    """
    Run all transformation steps.

    Args:
        extract_dir: Directory containing extracted data
                     (defaults to settings.PUBLISH_DIRECTORY)
        output_dir: Directory for transformed data
                    (defaults to extract_dir/transformed)

    Returns:
        Dictionary with all transformation results
    """
    # Set defaults
    if extract_dir is None:
        extract_dir = Path(settings.PUBLISH_DIRECTORY)

    if output_dir is None:
        output_dir = extract_dir / "transformed"

    logger.info("=" * 60)
    logger.info("Starting Data Transformations")
    logger.info("=" * 60)
    logger.info(f"Extract directory: {extract_dir}")
    logger.info(f"Output directory: {output_dir}")

    all_results = {}

    # Step 1: Consolidate SRA entities
    try:
        logger.info("\nStep 1: Consolidating SRA entities...")
        sra_results = consolidate_sra_entities(extract_dir, output_dir)
        all_results["sra_consolidation"] = sra_results
    except Exception as e:
        logger.error(f"SRA consolidation failed: {e}")
        all_results["sra_consolidation"] = {"error": str(e)}

    # Step 2: Custom transformations
    try:
        logger.info("\nStep 2: Running custom transformations...")
        custom_results = run_custom_transformations(extract_dir, output_dir)
        all_results["custom_transformations"] = custom_results
    except Exception as e:
        logger.error(f"Custom transformations failed: {e}")
        all_results["custom_transformations"] = {"error": str(e)}

    logger.info("=" * 60)
    logger.info("Transformation Summary")
    logger.info("=" * 60)

    for step_name, step_results in all_results.items():
        logger.info(f"\n{step_name}:")
        if isinstance(step_results, dict):
            for key, value in step_results.items():
                if isinstance(value, int):
                    logger.info(f"  {key}: {value:,} rows")
                else:
                    logger.info(f"  {key}: {value}")

    logger.info("=" * 60)

    return all_results


# CLI Interface

@click.group()
def transform():
    """Data transformation commands."""
    pass


@transform.command()
@click.option(
    '--extract-dir',
    type=click.Path(path_type=Path),
    default=None,
    help=f'Directory containing extracted data (default: {settings.PUBLISH_DIRECTORY})'
)
@click.option(
    '--output-dir',
    type=click.Path(path_type=Path),
    default=None,
    help='Directory for transformed data (default: extract-dir/transformed)'
)
def run(extract_dir: Optional[Path], output_dir: Optional[Path]):
    """
    Run all data transformations.

    This consolidates chunked parquet files and runs custom transformations.
    Output is written to the transformed/ directory.

    Examples:

        # Use default directories from config
        oidx transform run

        # Specify custom directories
        oidx transform run --extract-dir /data/omicidx --output-dir /data/omicidx/transformed
    """
    try:
        results = run_all_transformations(extract_dir, output_dir)

        # Check for errors
        errors = [
            step for step, result in results.items()
            if isinstance(result, dict) and "error" in result
        ]

        if errors:
            logger.error(f"Transformations completed with errors in: {', '.join(errors)}")
            raise click.Abort()
        else:
            logger.info("✓ All transformations completed successfully")

    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise click.Abort()


@transform.command()
@click.option(
    '--extract-dir',
    type=click.Path(path_type=Path),
    default=None,
    help=f'Directory containing extracted data (default: {settings.PUBLISH_DIRECTORY})'
)
@click.option(
    '--output-dir',
    type=click.Path(path_type=Path),
    default=None,
    help='Directory for consolidated data (default: extract-dir/transformed)'
)
def consolidate(extract_dir: Optional[Path], output_dir: Optional[Path]):
    """
    Consolidate chunked SRA parquet files only.

    This combines all experiment/run/sample/study chunks into single files
    per entity type. Useful for running consolidation separately from other
    transformations.
    """
    if extract_dir is None:
        extract_dir = Path(settings.PUBLISH_DIRECTORY)

    if output_dir is None:
        output_dir = extract_dir / "transformed"

    logger.info(f"Consolidating SRA files from {extract_dir}")

    try:
        results = consolidate_sra_entities(extract_dir, output_dir)

        logger.info("\nConsolidation Summary:")
        for entity, count in results.items():
            logger.info(f"  {entity}: {count:,} rows")

        logger.info(f"\n✓ Consolidation complete. Files in: {output_dir}")

    except Exception as e:
        logger.error(f"Consolidation failed: {e}")
        raise click.Abort()


if __name__ == "__main__":
    transform()
