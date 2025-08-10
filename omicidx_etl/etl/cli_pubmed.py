import click
import logging
from pathlib import Path
from datetime import datetime

from .pubmed import (
    etl_pubmeds,
)


@click.group()
def pubmed():
    """PubMed ETL commands."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
@pubmed.command()
def etl():
    start_time = datetime.now()
    
    try:        
        click.echo("Starting PubMed ETL process...")
        etl_pubmeds()
        duration = datetime.now() - start_time
        click.echo(f"✓ ETL completed in {duration}")
    except Exception as e:
        click.echo(f"✗ ETL failed: {e}", err=True)
        raise click.Abort()