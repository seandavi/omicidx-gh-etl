"""
Data transformation module using DuckDB.

This module orchestrates transformations across different data sources
using modular transformation functions organized by domain.
"""

from pathlib import Path
from typing import Optional
import click
from loguru import logger
from .db import duckdb_connection
from .config import settings
from .transformations import sra, biosample, geo


def run_all_transformations(
    extract_dir: Optional[Path] = None,
    output_dir: Optional[Path] = None
) -> dict[str, any]:
    """
    Run all data transformations.

    This consolidates chunked parquet/NDJSON files and creates summary tables.
    Transformations are organized by data source for modularity.

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

    output_dir.mkdir(parents=True, exist_ok=True)

    all_results = {}

    with duckdb_connection() as con:
        # Stage 1: SRA Transformations
        logger.info("\n" + "=" * 60)
        logger.info("Stage 1: SRA Consolidation")
        logger.info("=" * 60)

        try:
            sra_results = sra.consolidate_entities(con, extract_dir, output_dir)
            all_results["sra_consolidation"] = sra_results

            # Create SRA summary if consolidation succeeded
            if sra_results and any(sra_results.values()):
                sra_summary_count = sra.create_study_summary(con, output_dir)
                all_results["sra_study_summary"] = sra_summary_count
        except Exception as e:
            logger.error(f"SRA transformations failed: {e}")
            all_results["sra_consolidation"] = {"error": str(e)}

        # Stage 2: Biosample Transformations
        logger.info("\n" + "=" * 60)
        logger.info("Stage 2: Biosample/Bioproject Consolidation")
        logger.info("=" * 60)

        try:
            biosample_count = biosample.consolidate_biosamples(con, extract_dir, output_dir)
            all_results["biosamples"] = biosample_count

            bioproject_count = biosample.consolidate_bioprojects(con, extract_dir, output_dir)
            all_results["bioprojects"] = bioproject_count

            ebi_biosample_count = biosample.consolidate_ebi_biosamples(con, extract_dir, output_dir)
            all_results["ebi_biosamples"] = ebi_biosample_count
        except Exception as e:
            logger.error(f"Biosample transformations failed: {e}")
            all_results["biosample_consolidation"] = {"error": str(e)}

        # Stage 3: GEO Transformations
        logger.info("\n" + "=" * 60)
        logger.info("Stage 3: GEO Consolidation")
        logger.info("=" * 60)

        try:
            gse_count = geo.consolidate_gse(con, extract_dir, output_dir)
            all_results["geo_series"] = gse_count

            gsm_count = geo.consolidate_gsm(con, extract_dir, output_dir)
            all_results["geo_samples"] = gsm_count

            gpl_count = geo.consolidate_gpl(con, extract_dir, output_dir)
            all_results["geo_platforms"] = gpl_count
        except Exception as e:
            logger.error(f"GEO transformations failed: {e}")
            all_results["geo_consolidation"] = {"error": str(e)}

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("Transformation Summary")
    logger.info("=" * 60)

    # SRA
    logger.info("\nSRA:")
    if "sra_consolidation" in all_results:
        sra_results = all_results["sra_consolidation"]
        if isinstance(sra_results, dict) and "error" not in sra_results:
            for entity, count in sra_results.items():
                if count:
                    logger.info(f"  {entity}: {count:,} rows")
        elif isinstance(sra_results, dict) and "error" in sra_results:
            logger.error(f"  Error: {sra_results['error']}")

    if "sra_study_summary" in all_results:
        logger.info(f"  study_summary: {all_results['sra_study_summary']:,} rows")

    # Biosample
    logger.info("\nBiosample:")
    for key in ["biosamples", "bioprojects", "ebi_biosamples"]:
        if key in all_results and all_results[key]:
            logger.info(f"  {key}: {all_results[key]:,} rows")

    # GEO
    logger.info("\nGEO:")
    for key in ["geo_series", "geo_samples", "geo_platforms"]:
        if key in all_results and all_results[key]:
            logger.info(f"  {key}: {all_results[key]:,} rows")

    logger.info("\n" + "=" * 60)
    logger.info(f"Output directory: {output_dir}")
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

    This consolidates chunked parquet files and creates summary tables.
    Organized by data source:
    - SRA: studies, experiments, samples, runs + summary
    - Biosample: biosamples, bioprojects, ebi_biosamples
    - GEO: series (GSE), samples (GSM), platforms (GPL)

    Examples:

        # Use default directories from config
        oidx transform run

        # Specify custom directories
        oidx transform run --extract-dir /data/omicidx --output-dir /data/omicidx/transformed
    """
    try:
        results = run_all_transformations(extract_dir, output_dir)

        # Check for errors
        errors = []
        for key, value in results.items():
            if isinstance(value, dict) and "error" in value:
                errors.append(key)

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
@click.option(
    '--source',
    type=click.Choice(['sra', 'biosample', 'geo', 'all']),
    default='all',
    help='Which data source to consolidate (default: all)'
)
def consolidate(extract_dir: Optional[Path], output_dir: Optional[Path], source: str):
    """
    Consolidate chunked files for specific data sources.

    Useful for running consolidation separately or for specific sources.

    Examples:

        # Consolidate all sources
        oidx transform consolidate

        # Consolidate only SRA
        oidx transform consolidate --source sra

        # Consolidate only GEO
        oidx transform consolidate --source geo
    """
    if extract_dir is None:
        extract_dir = Path(settings.PUBLISH_DIRECTORY)

    if output_dir is None:
        output_dir = extract_dir / "transformed"

    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Consolidating {source} files from {extract_dir}")

    try:
        with duckdb_connection() as con:
            if source in ['sra', 'all']:
                logger.info("\nConsolidating SRA...")
                sra_results = sra.consolidate_entities(con, extract_dir, output_dir)
                for entity, count in sra_results.items():
                    if count:
                        logger.info(f"  {entity}: {count:,} rows")

            if source in ['biosample', 'all']:
                logger.info("\nConsolidating Biosample/Bioproject...")
                biosample_count = biosample.consolidate_biosamples(con, extract_dir, output_dir)
                bioproject_count = biosample.consolidate_bioprojects(con, extract_dir, output_dir)
                ebi_count = biosample.consolidate_ebi_biosamples(con, extract_dir, output_dir)

                if biosample_count:
                    logger.info(f"  biosamples: {biosample_count:,} rows")
                if bioproject_count:
                    logger.info(f"  bioprojects: {bioproject_count:,} rows")
                if ebi_count:
                    logger.info(f"  ebi_biosamples: {ebi_count:,} rows")

            if source in ['geo', 'all']:
                logger.info("\nConsolidating GEO...")
                gse_count = geo.consolidate_gse(con, extract_dir, output_dir)
                gsm_count = geo.consolidate_gsm(con, extract_dir, output_dir)
                gpl_count = geo.consolidate_gpl(con, extract_dir, output_dir)

                if gse_count:
                    logger.info(f"  geo_series: {gse_count:,} rows")
                if gsm_count:
                    logger.info(f"  geo_samples: {gsm_count:,} rows")
                if gpl_count:
                    logger.info(f"  geo_platforms: {gpl_count:,} rows")

        logger.info(f"\n✓ Consolidation complete. Files in: {output_dir}")

    except Exception as e:
        logger.error(f"Consolidation failed: {e}")
        raise click.Abort()


if __name__ == "__main__":
    transform()
