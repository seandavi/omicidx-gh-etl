"""
CLI interface for SRA extraction.
"""

import click
from loguru import logger
from pathlib import Path
from .extract import (
    extract_sra, 
    get_file_stats, 
    OUTPUT_FORMAT_PARQUET,
    OUTPUT_FORMAT_NDJSON
)


@click.group()
def sra():
    """SRA extraction commands."""
    pass


@sra.command()
@click.argument('output_dir', type=click.Path(path_type=Path))
@click.option('--format', 'output_format', 
              type=click.Choice([OUTPUT_FORMAT_PARQUET, OUTPUT_FORMAT_NDJSON]),
              default=OUTPUT_FORMAT_PARQUET,
              help='Output format (default: parquet)')
@click.option('--workers', default=4, help='Number of parallel workers (default: 4)')
@click.option('--include-accessions/--no-accessions', default=True, 
              help='Include SRA accessions extraction (default: yes)')
def extract(output_dir: Path, output_format: str, workers: int, include_accessions: bool):
    """Extract SRA data to local files."""
    logger.info(f"Starting SRA extraction to {output_dir}")
    logger.info(f"Format: {output_format}, Workers: {workers}, Accessions: {include_accessions}")
    
    results = extract_sra(
        output_dir, max_workers=workers, 
        output_format=output_format, include_accessions=include_accessions
    )
    
    return results

@sra.command()
@click.argument('output_dir', type=click.Path(exists=True, path_type=Path))
@click.option('--format', 'output_format',
              type=click.Choice([OUTPUT_FORMAT_PARQUET, OUTPUT_FORMAT_NDJSON]),
              default=OUTPUT_FORMAT_PARQUET,
              help='File format to analyze (default: parquet)')
def stats(output_dir: Path, output_format: str):
    """Show statistics about extracted files."""
    stats_data = get_file_stats(output_dir, output_format)
    
    for entity, info in stats_data.items():
        click.echo(f"\n{entity.upper()} Files:")
        click.echo(f"  Count: {info['file_count']}")
        click.echo(f"  Total Size: {info['total_size_mb']:.2f} MB")
        
        if info['files']:
            click.echo("  Files:")
            for filename in info['files'][:10]:  # Show first 10 files
                click.echo(f"    - {filename}")
            if len(info['files']) > 10:
                click.echo(f"    ... and {len(info['files']) - 10} more")



