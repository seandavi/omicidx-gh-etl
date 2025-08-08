"""
Main CLI entry point for omicidx-etl.
"""

import click
from omicidx_etl.biosample.cli import biosample


@click.group()
@click.version_option()
def cli():
    """OmicIDX ETL Pipeline - Simplified data extraction tools."""
    pass


# Add subcommands
cli.add_command(biosample)


if __name__ == "__main__":
    cli()
