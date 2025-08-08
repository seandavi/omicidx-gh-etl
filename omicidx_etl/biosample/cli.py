"""
CLI for biosample/bioproject extraction.
"""

import click
import logging
from pathlib import Path
from datetime import datetime

# Import our extraction functions
from .extract import (
    extract_biosample, 
    extract_bioproject, 
    get_file_stats,
    upload_to_r2,
    extract_and_upload
)


@click.group()
def biosample():
    """Biosample/Bioproject ETL commands."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


@biosample.command()
@click.option(
    "--output-dir", 
    default="/data/omicidx/biosample", 
    help="Output directory for extracted files"
)
@click.option(
    "--entity", 
    type=click.Choice(["biosample", "bioproject", "both"]), 
    default="both",
    help="Which entity to extract"
)
@click.option(
    "--upload/--no-upload", 
    default=False,
    help="Upload to R2 storage after extraction"
)
def extract(output_dir: str, entity: str, upload: bool):
    """Extract biosample/bioproject data to NDJSON files."""
    output_path = Path(output_dir)
    start_time = datetime.now()
    
    click.echo(f"Starting extraction to {output_path}")
    click.echo(f"Entity: {entity}, Upload: {upload}")
    
    try:
        if entity == "both":
            results = extract_and_upload(output_path, upload)
            for ent, files in results.items():
                click.echo(f"✓ {ent}: {len(files)} files created")
        elif entity == "biosample":
            files = extract_biosample(output_path)
            if upload:
                upload_to_r2(files, "biosample")
            click.echo(f"✓ biosample: {len(files)} files created")
        elif entity == "bioproject":
            files = extract_bioproject(output_path)
            if upload:
                upload_to_r2(files, "bioproject")
            click.echo(f"✓ bioproject: {len(files)} files created")
        
        duration = datetime.now() - start_time
        click.echo(f"✓ Completed in {duration}")
        
    except Exception as e:
        click.echo(f"✗ Error: {e}", err=True)
        raise click.Abort()


@biosample.command()
@click.option(
    "--output-dir", 
    default="/data/omicidx/biosample",
    help="Directory containing extracted files"
)
def stats(output_dir: str):
    """Show statistics about extracted files."""
    output_path = Path(output_dir)
    
    if not output_path.exists():
        click.echo(f"Directory {output_path} does not exist", err=True)
        return
    
    stats_data = get_file_stats(output_path)
    
    if not any(info['file_count'] > 0 for info in stats_data.values()):
        click.echo("No extracted files found")
        return
    
    click.echo(f"File statistics for {output_path}:")
    click.echo("-" * 50)
    
    for entity, info in stats_data.items():
        if info['file_count'] > 0:
            click.echo(f"\n{entity.upper()}:")
            click.echo(f"  Files: {info['file_count']}")
            click.echo(f"  Total size: {info['total_size_mb']:.1f} MB")
            click.echo("  Files:")
            for filename in info['files']:
                click.echo(f"    {filename}")


@biosample.command()
@click.argument("entity", type=click.Choice(["biosample", "bioproject"]))
@click.option(
    "--output-dir", 
    default="/data/omicidx/biosample",
    help="Directory containing files to upload"
)
@click.option(
    "--bucket",
    default="biodatalake",
    help="R2 bucket name"
)
def upload(entity: str, output_dir: str, bucket: str):
    """Upload existing local files to R2 storage."""
    output_path = Path(output_dir)
    
    if not output_path.exists():
        click.echo(f"Directory {output_path} does not exist", err=True)
        return
    
    files = sorted(output_path.glob(f"{entity}-*.ndjson.gz"))
    
    if not files:
        click.echo(f"No {entity} files found in {output_path}")
        return
    
    click.echo(f"Uploading {len(files)} {entity} files to R2...")
    
    try:
        upload_to_r2(files, entity, bucket)
        click.echo(f"✓ Successfully uploaded {len(files)} files")
    except Exception as e:
        click.echo(f"✗ Upload failed: {e}", err=True)
        raise click.Abort()


@biosample.command()
@click.option(
    "--output-dir", 
    default="/data/omicidx/biosample",
    help="Directory to clean"
)
@click.option(
    "--entity",
    type=click.Choice(["biosample", "bioproject", "both"]),
    default="both",
    help="Which entity files to clean"
)
@click.confirmation_option(
    prompt="Are you sure you want to delete extracted files?"
)
def clean(output_dir: str, entity: str):
    """Clean up extracted files."""
    output_path = Path(output_dir)
    
    if not output_path.exists():
        click.echo(f"Directory {output_path} does not exist")
        return
    
    entities = ["biosample", "bioproject"] if entity == "both" else [entity]
    total_deleted = 0
    
    for ent in entities:
        files = list(output_path.glob(f"{ent}-*.ndjson.gz"))
        for file_path in files:
            file_path.unlink()
            total_deleted += 1
        click.echo(f"Deleted {len(files)} {ent} files")
    
    click.echo(f"✓ Total files deleted: {total_deleted}")


if __name__ == "__main__":
    biosample()
