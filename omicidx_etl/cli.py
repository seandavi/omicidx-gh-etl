"""
Main CLI entry point for omicidx-etl.
"""

import click
from omicidx_etl.biosample.extract import biosample
from omicidx_etl.ebi_biosample.extract import ebi_biosample
from omicidx_etl.sra.cli import sra
from omicidx_etl.geo.extract import geo
from omicidx_etl.etl.icite import icite
from omicidx_etl.etl.pubmed import pubmed
from omicidx_etl.etl.europepmc_textmined import europepmc
from .nih_reporter import nih_reporter


@click.group()
@click.version_option()
def cli():
    """OmicIDX ETL Pipeline - Simplified data extraction tools."""
    pass


# Add subcommands
cli.add_command(biosample)
cli.add_command(sra)
cli.add_command(ebi_biosample)
cli.add_command(geo)
cli.add_command(icite)
cli.add_command(pubmed)
cli.add_command(europepmc)
cli.add_command(nih_reporter)

if __name__ == "__main__":
    cli()
