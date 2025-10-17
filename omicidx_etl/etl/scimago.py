import pandas
import re
from pathlib import Path
import click
from loguru import logger
from upath import UPath


SCIMAGO_URL = "https://www.scimagojr.com/journalrank.php?out=xls"


def fetch_and_save_scimago(output_path: UPath) -> None:
    """Fetch Scimago Journal Impact Factors and save to NDJSON format.

    Args:
        output_path: Directory where the output file will be saved
    """
    logger.info("Fetching Scimago Journal Impact Factors")

    # Fetch the data
    scimago = pandas.read_csv(SCIMAGO_URL, delimiter=";")

    # Clean column names
    scimago.rename(
        lambda x: re.sub(r"[^\w\d_]+", "_", x.lower()).strip("_"),
        axis="columns",
        inplace=True,
    )

    logger.info(f"Fetched {len(scimago)} journal records")

    # Ensure output directory exists
    output_path.mkdir(parents=True, exist_ok=True)

    # Save to NDJSON format
    output_file = output_path / "scimago.ndjson.gz"
    scimago.to_json(output_file, orient="records", lines=True, compression="gzip")

    logger.info(f"Saved Scimago Journal Impact Factors to {output_file}")


@click.group()
def scimago():
    """OmicIDX ETL Pipeline - Scimago journal impact factors."""
    pass


@scimago.command()
@click.argument("output_dir", type=UPath)
def extract(output_dir: UPath):
    """Extract Scimago journal impact factors.

    Args:
        output_dir: Directory where the output file will be saved
    """
    logger.info(f"Starting Scimago extraction to {output_dir}")
    fetch_and_save_scimago(output_dir)


if __name__ == "__main__":
    scimago()
