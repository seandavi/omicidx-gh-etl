"""
CLI interface for SRA extraction.
"""

import click
from pathlib import Path
from .extract import (
    extract_sra, 
    extract_and_upload, 
    get_file_stats, 
    upload_to_r2,
    OUTPUT_FORMAT_PARQUET,
    OUTPUT_FORMAT_NDJSON
)
from ..logging import get_logger

logger = get_logger(__name__)


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
@click.option('--upload/--no-upload', default=False, help='Upload to R2 after extraction')
def extract(output_dir: Path, output_format: str, workers: int, upload: bool):
    """Extract SRA data to local files."""
    logger.info(f"Starting SRA extraction to {output_dir}")
    logger.info(f"Format: {output_format}, Workers: {workers}, Upload: {upload}")
    
    if upload:
        results = extract_and_upload(output_dir, upload=True, max_workers=workers, output_format=output_format)
    else:
        results = extract_sra(output_dir, max_workers=workers, output_format=output_format)
    
    if results:
        total_records = sum(results.values())
        logger.info(f"Extraction completed: {len(results)} files, {total_records} total records")
    else:
        logger.warning("No files were extracted")


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


@sra.command()
@click.argument('input_dir', type=click.Path(exists=True, path_type=Path))
@click.option('--bucket', default='biodatalake', help='R2 bucket name')
def upload(input_dir: Path, bucket: str):
    """Upload extracted files to R2 storage."""
    # Find files to upload (both formats)
    parquet_files = list(input_dir.glob("*.parquet"))
    ndjson_files = list(input_dir.glob("*.ndjson.gz"))
    
    all_files = parquet_files + ndjson_files
    
    if not all_files:
        click.echo("No files found to upload")
        return
    
    logger.info(f"Uploading {len(all_files)} files to R2 bucket: {bucket}")
    upload_to_r2(all_files, bucket)
    logger.info("Upload completed")


@sra.command()
@click.argument('output_dir', type=click.Path(path_type=Path))
@click.option('--format', 'output_format',
              type=click.Choice([OUTPUT_FORMAT_PARQUET, OUTPUT_FORMAT_NDJSON, 'all']),
              default='all',
              help='File format to clean (default: all)')
def clean(output_dir: Path, output_format: str):
    """Clean up old extracted files."""
    if not output_dir.exists():
        click.echo(f"Directory {output_dir} does not exist")
        return
    
    patterns = []
    if output_format == 'all':
        patterns = ["*.parquet", "*.ndjson.gz"]
    elif output_format == OUTPUT_FORMAT_PARQUET:
        patterns = ["*.parquet"]
    else:
        patterns = ["*.ndjson.gz"]
    
    total_removed = 0
    for pattern in patterns:
        files = list(output_dir.glob(pattern))
        for file_path in files:
            file_path.unlink()
            total_removed += 1
        
        if files:
            logger.info(f"Removed {len(files)} {pattern} files")
    
    if total_removed > 0:
        logger.info(f"Total files removed: {total_removed}")
    else:
        click.echo("No files found to clean")
