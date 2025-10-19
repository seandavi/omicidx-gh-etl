"""
Biosample and Bioproject data transformations.

Functions for consolidating NCBI Biosample and Bioproject data.
"""

from pathlib import Path
from loguru import logger


def consolidate_biosamples(con, extract_dir: Path, output_dir: Path) -> int:
    """
    Consolidate chunked biosample parquet files into a single file.

    Args:
        con: DuckDB connection
        extract_dir: Directory containing extracted biosample data
        output_dir: Directory where consolidated file will be written

    Returns:
        Number of biosamples consolidated
    """
    biosample_dir = extract_dir / "biosample"

    if not biosample_dir.exists():
        logger.warning(f"Biosample directory not found: {biosample_dir}")
        return 0

    logger.info(f"Consolidating biosample data from {biosample_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    pattern = str(biosample_dir / "*biosample*.parquet")
    output_path = output_dir / "biosamples.parquet"

    try:
        # Check if files exist
        file_count = con.execute(f"""
            SELECT COUNT(*) as cnt
            FROM glob('{pattern}')
        """).fetchone()[0]

        if file_count == 0:
            logger.warning(f"No biosample files found matching pattern: {pattern}")
            return 0

        logger.info(f"Found {file_count} biosample file(s)")

        # Consolidate all chunks
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

        logger.info(f"✓ Created biosamples.parquet with {row_count:,} rows")
        return row_count

    except Exception as e:
        logger.error(f"Failed to consolidate biosamples: {e}")
        return 0


def consolidate_bioprojects(con, extract_dir: Path, output_dir: Path) -> int:
    """
    Consolidate chunked bioproject parquet files into a single file.

    Args:
        con: DuckDB connection
        extract_dir: Directory containing extracted bioproject data
        output_dir: Directory where consolidated file will be written

    Returns:
        Number of bioprojects consolidated
    """
    biosample_dir = extract_dir / "biosample"

    if not biosample_dir.exists():
        logger.warning(f"Biosample directory not found: {biosample_dir}")
        return 0

    logger.info(f"Consolidating bioproject data from {biosample_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    pattern = str(biosample_dir / "*bioproject*.parquet")
    output_path = output_dir / "bioprojects.parquet"

    try:
        # Check if files exist
        file_count = con.execute(f"""
            SELECT COUNT(*) as cnt
            FROM glob('{pattern}')
        """).fetchone()[0]

        if file_count == 0:
            logger.warning(f"No bioproject files found matching pattern: {pattern}")
            return 0

        logger.info(f"Found {file_count} bioproject file(s)")

        # Consolidate all chunks
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

        logger.info(f"✓ Created bioprojects.parquet with {row_count:,} rows")
        return row_count

    except Exception as e:
        logger.error(f"Failed to consolidate bioprojects: {e}")
        return 0


def consolidate_ebi_biosamples(con, extract_dir: Path, output_dir: Path) -> int:
    """
    Consolidate EBI biosample data from NDJSON.gz files to parquet.

    EBI biosamples are stored as NDJSON.gz files organized by date range.
    This consolidates them into a single parquet file.

    Args:
        con: DuckDB connection
        extract_dir: Directory containing extracted EBI biosample data
        output_dir: Directory where consolidated file will be written

    Returns:
        Number of EBI biosamples consolidated
    """
    ebi_biosample_dir = extract_dir / "ebi_biosample"

    if not ebi_biosample_dir.exists():
        logger.warning(f"EBI biosample directory not found: {ebi_biosample_dir}")
        return 0

    logger.info(f"Consolidating EBI biosample data from {ebi_biosample_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    pattern = str(ebi_biosample_dir / "biosamples-*.ndjson.gz")
    output_path = output_dir / "ebi_biosamples.parquet"

    try:
        # Check if files exist
        file_count = con.execute(f"""
            SELECT COUNT(*) as cnt
            FROM glob('{pattern}')
        """).fetchone()[0]

        if file_count == 0:
            logger.warning(f"No EBI biosample files found matching pattern: {pattern}")
            return 0

        logger.info(f"Found {file_count} EBI biosample file(s)")

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

        logger.info(f"✓ Created ebi_biosamples.parquet with {row_count:,} rows")
        return row_count

    except Exception as e:
        logger.error(f"Failed to consolidate EBI biosamples: {e}")
        return 0
