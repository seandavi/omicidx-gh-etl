"""
Main CLI entry point for omicidx-etl.
"""

import click
from omicidx_etl.biosample.extract import biosample
from omicidx_etl.ebi_biosample.extract import ebi_biosample
from omicidx_etl.sra.cli import sra
from omicidx_etl.geo.extract import geo


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


if __name__ == "__main__":
    cli()
