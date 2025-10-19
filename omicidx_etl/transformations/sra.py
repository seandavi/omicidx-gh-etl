"""
SRA data transformations.

Functions for consolidating and transforming SRA (Sequence Read Archive) data.
"""

from pathlib import Path
from loguru import logger


def consolidate_entities(con, extract_dir: Path, output_dir: Path) -> dict[str, int]:
    """
    Consolidate chunked SRA parquet files into single files per entity type.

    SRA extraction creates chunked files like:
    - 20251001_Full-study-0001.parquet
    - 20251001_Full-study-0002.parquet
    - 20251002-study-0001.parquet
    - etc.

    This consolidates them into:
    - studies.parquet
    - experiments.parquet
    - samples.parquet
    - runs.parquet

    Args:
        con: DuckDB connection
        extract_dir: Directory containing extracted SRA data
        output_dir: Directory where consolidated files will be written

    Returns:
        Dictionary with entity type and row counts
    """
    sra_dir = extract_dir / "sra"

    if not sra_dir.exists():
        logger.warning(f"SRA directory not found: {sra_dir}")
        return {}

    logger.info(f"Consolidating SRA data from {sra_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    results = {}

    # Define entity types to consolidate
    entities = {
        "study": "studies",
        "experiment": "experiments",
        "sample": "samples",
        "run": "runs"
    }

    for entity_singular, entity_plural in entities.items():
        logger.info(f"Consolidating {entity_singular} files...")

        # Pattern to match all chunks for this entity
        pattern = str(sra_dir / f"*{entity_singular}*.parquet")

        # Check if files exist
        try:
            file_count = con.execute(f"""
                SELECT COUNT(*) as cnt
                FROM glob('{pattern}')
            """).fetchone()[0]

            if file_count == 0:
                logger.warning(f"No {entity_singular} files found matching pattern: {pattern}")
                results[entity_plural] = 0
                continue

            logger.info(f"Found {file_count} {entity_singular} file(s)")

            # Consolidate all chunks into single table
            output_path = output_dir / f"{entity_plural}.parquet"

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

            results[entity_plural] = row_count
            logger.info(f"✓ Created {entity_plural}.parquet with {row_count:,} rows")

        except Exception as e:
            logger.error(f"Failed to consolidate {entity_singular}: {e}")
            results[entity_plural] = None

    return results


def create_study_summary(con, output_dir: Path) -> int:
    """
    Create a summary table with experiment/run counts per study.

    Args:
        con: DuckDB connection
        output_dir: Directory containing consolidated parquet files

    Returns:
        Number of studies in summary
    """
    logger.info("Creating SRA study summary")

    studies_path = output_dir / "studies.parquet"
    experiments_path = output_dir / "experiments.parquet"
    runs_path = output_dir / "runs.parquet"

    # Check required files exist
    if not all([studies_path.exists(), experiments_path.exists(), runs_path.exists()]):
        logger.warning("Missing required consolidated files for study summary")
        return 0

    output_path = output_dir / "sra_study_summary.parquet"

    try:
        con.execute(f"""
            COPY (
                SELECT
                    s.accession as study_accession,
                    s.title as study_title,
                    COUNT(DISTINCT e.accession) as experiment_count,
                    COUNT(DISTINCT r.accession) as run_count,
                    SUM(r.total_bases) as total_bases,
                    SUM(r.total_spots) as total_spots
                FROM read_parquet('{studies_path}') s
                LEFT JOIN read_parquet('{experiments_path}') e
                    ON s.accession = e.study_accession
                LEFT JOIN read_parquet('{runs_path}') r
                    ON e.accession = r.experiment_accession
                GROUP BY s.accession, s.title
            ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        row_count = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{output_path}')
        """).fetchone()[0]

        logger.info(f"✓ Created sra_study_summary.parquet with {row_count:,} studies")
        return row_count

    except Exception as e:
        logger.error(f"Failed to create study summary: {e}")
        return 0
