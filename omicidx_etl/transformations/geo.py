"""
GEO (Gene Expression Omnibus) data transformations.

Functions for consolidating GEO data including GSE (series), GSM (samples),
and GPL (platforms).
"""

from pathlib import Path
from loguru import logger


def consolidate_gse(con, extract_dir: Path, output_dir: Path) -> int:
    """
    Consolidate GEO Series (GSE) files into a single parquet file.

    GSE files contain information about GEO series/studies.

    Args:
        con: DuckDB connection
        extract_dir: Directory containing extracted GEO data
        output_dir: Directory where consolidated file will be written

    Returns:
        Number of GSE records consolidated
    """
    geo_dir = extract_dir / "geo"

    if not geo_dir.exists():
        logger.warning(f"GEO directory not found: {geo_dir}")
        return 0

    logger.info(f"Consolidating GSE (series) data from {geo_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    pattern = str(geo_dir / "gse-*.ndjson.gz")
    output_path = output_dir / "geo_series.parquet"

    try:
        # Check if files exist
        file_count = con.execute(f"""
            SELECT COUNT(*) as cnt
            FROM glob('{pattern}')
        """).fetchone()[0]

        if file_count == 0:
            logger.warning(f"No GSE files found matching pattern: {pattern}")
            return 0

        logger.info(f"Found {file_count} GSE file(s)")

        # Consolidate NDJSON files to parquet
        con.execute(f"""
            COPY (
                SELECT * FROM read_json('{pattern}', format='newline_delimited')
            ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        # Get row count
        row_count = con.execute(f"""
            SELECT COUNT(*) as cnt
            FROM read_parquet('{output_path}')
        """).fetchone()[0]

        logger.info(f"✓ Created geo_series.parquet with {row_count:,} rows")
        return row_count

    except Exception as e:
        logger.error(f"Failed to consolidate GSE: {e}")
        return 0


def consolidate_gsm(con, extract_dir: Path, output_dir: Path) -> int:
    """
    Consolidate GEO Samples (GSM) files into a single parquet file.

    GSM files contain information about individual GEO samples.

    Args:
        con: DuckDB connection
        extract_dir: Directory containing extracted GEO data
        output_dir: Directory where consolidated file will be written

    Returns:
        Number of GSM records consolidated
    """
    geo_dir = extract_dir / "geo"

    if not geo_dir.exists():
        logger.warning(f"GEO directory not found: {geo_dir}")
        return 0

    logger.info(f"Consolidating GSM (samples) data from {geo_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    pattern = str(geo_dir / "gsm-*.ndjson.gz")
    output_path = output_dir / "geo_samples.parquet"

    try:
        # Check if files exist
        file_count = con.execute(f"""
            SELECT COUNT(*) as cnt
            FROM glob('{pattern}')
        """).fetchone()[0]

        if file_count == 0:
            logger.warning(f"No GSM files found matching pattern: {pattern}")
            return 0

        logger.info(f"Found {file_count} GSM file(s)")

        # Consolidate NDJSON files to parquet
        con.execute(f"""
            COPY (
                SELECT * FROM read_json('{pattern}', format='newline_delimited')
            ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        # Get row count
        row_count = con.execute(f"""
            SELECT COUNT(*) as cnt
            FROM read_parquet('{output_path}')
        """).fetchone()[0]

        logger.info(f"✓ Created geo_samples.parquet with {row_count:,} rows")
        return row_count

    except Exception as e:
        logger.error(f"Failed to consolidate GSM: {e}")
        return 0


def consolidate_gpl(con, extract_dir: Path, output_dir: Path) -> int:
    """
    Consolidate GEO Platforms (GPL) files into a single parquet file.

    GPL files contain information about microarray/sequencing platforms.

    Args:
        con: DuckDB connection
        extract_dir: Directory containing extracted GEO data
        output_dir: Directory where consolidated file will be written

    Returns:
        Number of GPL records consolidated
    """
    geo_dir = extract_dir / "geo"

    if not geo_dir.exists():
        logger.warning(f"GEO directory not found: {geo_dir}")
        return 0

    logger.info(f"Consolidating GPL (platforms) data from {geo_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    pattern = str(geo_dir / "gpl-*.ndjson.gz")
    output_path = output_dir / "geo_platforms.parquet"

    try:
        # Check if files exist
        file_count = con.execute(f"""
            SELECT COUNT(*) as cnt
            FROM glob('{pattern}')
        """).fetchone()[0]

        if file_count == 0:
            logger.warning(f"No GPL files found matching pattern: {pattern}")
            return 0

        logger.info(f"Found {file_count} GPL file(s)")

        # Consolidate NDJSON files to parquet
        con.execute(f"""
            COPY (
                SELECT * FROM read_json('{pattern}', format='newline_delimited')
            ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        # Get row count
        row_count = con.execute(f"""
            SELECT COUNT(*) as cnt
            FROM read_parquet('{output_path}')
        """).fetchone()[0]

        logger.info(f"✓ Created geo_platforms.parquet with {row_count:,} rows")
        return row_count

    except Exception as e:
        logger.error(f"Failed to consolidate GPL: {e}")
        return 0
