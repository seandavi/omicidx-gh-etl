"""
Main CLI entry point for omicidx-etl.
"""

import click
from omicidx_etl.biosample.cli import biosample
from omicidx_etl.sra.cli import sra


@click.group()
@click.version_option()
def cli():
    """OmicIDX ETL Pipeline - Simplified data extraction tools."""
    pass


# Add subcommands
cli.add_command(biosample)
cli.add_command(sra)


if __name__ == "__main__":
    cli()
