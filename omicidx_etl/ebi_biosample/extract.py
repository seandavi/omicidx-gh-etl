from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from typing import Iterable
import tenacity
import anyio
import httpx
import orjson
from upath import UPath
import shutil
from ..config import settings
import click
from loguru import logger
import pyarrow as pa
import pyarrow.parquet as pq


output_dir = str(UPath(settings.PUBLISH_DIRECTORY) / "ebi_biosample")


def get_biosample_schema() -> pa.Schema:
    """Get the PyArrow schema for EBI BioSample records.

    Returns a schema that matches the structure of EBI BioSample API responses
    with flattened characteristics.
    """
    return pa.schema([
        pa.field("accession", pa.string()),
        pa.field("name", pa.string()),
        pa.field("update", pa.string()),
        pa.field("release", pa.string()),
        pa.field("create", pa.string()),
        pa.field("taxId", pa.int64()),
        pa.field("characteristics", pa.list_(pa.struct([
            pa.field("text", pa.string()),
            pa.field("ontologyTerms", pa.list_(pa.string())),
            pa.field("unit", pa.string()),
            pa.field("characteristic", pa.string())
        ]))),
        pa.field("organization", pa.list_(pa.struct([
            pa.field("Name", pa.string()),
            pa.field("Role", pa.string()),
            pa.field("Address", pa.string()),
            pa.field("URI", pa.string()),
            pa.field("Email", pa.string())
        ]))),
        pa.field("contact", pa.list_(pa.struct([
            pa.field("Name", pa.string()),
            pa.field("Role", pa.string()),
            pa.field("Email", pa.string())
        ]))),
        pa.field("publications", pa.list_(pa.struct([
            pa.field("pubmed_id", pa.string()),
            pa.field("doi", pa.string())
        ]))),
        pa.field("externalReferences", pa.list_(pa.struct([
            pa.field("url", pa.string()),
            pa.field("duo", pa.list_(pa.string()))
        ]))),
        pa.field("_links", pa.struct([
            pa.field("self", pa.struct([pa.field("href", pa.string())])),
            pa.field("curationLinks", pa.struct([pa.field("href", pa.string())])),
            pa.field("samples", pa.struct([pa.field("href", pa.string())])),
            pa.field("curationLink", pa.struct([pa.field("href", pa.string())]))
        ]))
    ])


def get_filename(
    start_date: date,
    end_date: date,
    tmp: bool = True,
    output_directory: str = output_dir
) -> str:
    """Get the filename for a given date range.

    The final filename looks like
    `biosamples-2021-01-01--2021-01-01--daily.parquet`, for example.
    """
    base = f"{output_directory}/biosamples-{start_date.strftime('%Y-%m-%d')}--{end_date.strftime('%Y-%m-%d')}--daily.parquet"
    if tmp:
        base += ".tmp"
    return base


BASEURL = "https://www.ebi.ac.uk/biosamples/samples"


class SampleFetcher:
    def __init__(
        self,
        cursor: str = "*",
        size: int = 200,
        start_date: date = date.today(),
        end_date: date = date.today(),
        output_directory: str = output_dir,
    ):
        self.cursor = cursor
        self.size = size
        self.start_date = start_date
        self.end_date = end_date
        self.output_directory = output_directory
        self.base_url = BASEURL
        self.full_url = None
        self.any_samples = False
        self.processed_count = 0
        self.samples_buffer = []  # Buffer samples in memory for Parquet writing

    def date_filter_string(self) -> str:
        """Get the filter string for a given date range.

        The EBI API uses a custom date filter syntax. This function
        returns a string that can be used in the `filter` parameter
        of the API request.
        """
        return f"""dt:update:from={self.start_date.strftime('%Y-%m-%d')}until={self.end_date.strftime('%Y-%m-%d')}"""

    @tenacity.retry(
        stop=tenacity.stop.stop_after_attempt(10),
        wait=tenacity.wait.wait_random_exponential(multiplier=1, max=40),
        before_sleep=lambda retry_state: logger.warning(
            f"request request failed, retrying in {retry_state.upcoming_sleep} seconds (attempt {retry_state.attempt_number}/5)"
        ),
    )
    async def perform_request(self) -> dict:
        """Perform a request to the EBI API with retries."""
        filt = self.date_filter_string()

        params = {
            "cursor": self.cursor,
            "size": self.size,
            "filter": filt,
        }
        logger.debug(f"Performing request to EBI API: {self.full_url if self.full_url is not None else self.base_url} with params {params}")
        async with httpx.AsyncClient() as client:
            if self.full_url is not None:
                response = await client.get(self.full_url, timeout=40)
            else:
                response = await client.get(self.base_url, params=params, timeout=40)
            response.raise_for_status()
            return response.json()

    async def fetch_next_set(self):
        """Fetch the next set of samples from the EBI API.

        This function fetches the next set of samples from the EBI API
        and yields them one by one. It also updates the cursor for the
        next request.
        """
        while True:
            try:
                response = await self.perform_request()
                for sample in response["_embedded"]["samples"]:
                    self.any_samples = True
                    characteristics = []
                    for k, v in sample["characteristics"].items():
                        for val in v:
                            val["characteristic"] = k
                            characteristics.append(val)
                    sample["characteristics"] = characteristics
                    yield sample

                if "next" in response["_links"]:
                    self.full_url = response["_links"]["next"]["href"]
                else:
                    self.completed()
                    break
            except KeyError: # no more samples
                self.completed()
                break

    async def process(self):
        """Process the samples from the EBI API.

        This function fetches samples from the EBI API and buffers them
        in memory. It runs in a loop until there are no more samples
        to fetch.
        """
        self.processed_count = 0

        async for sample in self.fetch_next_set():
            self.samples_buffer.append(sample)
            self.processed_count += 1
            if self.processed_count % 1000 == 0:
                logger.debug(f"Fetched {self.processed_count} samples so far for date range {self.start_date} to {self.end_date}")

    def completed(self):
        """Finalize the fetching process.

        This function is called when there are no more samples to fetch.
        """
        logger.info("Completed fetching samples")


def get_date_ranges(start_date_str: str, end_date_str: str) -> Iterable[tuple]:
    """Get date ranges for a given start and end date.

    Given a start and end date, returns a list of tuples representing daily date ranges.

    :param start_date_str: The start date in 'YYYY-MM-DD' format
    :param end_date_str: The end date in 'YYYY-MM-DD' format
    :return: Iterator of tuples, each containing a single day (same date for start and end)
    """
    # Convert strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    current_date = start_date

    while current_date <= end_date:
        # Yield single day range (start and end are the same)
        yield (current_date.date(), current_date.date())
        # Move to next day
        current_date = current_date + timedelta(days=1)


async def process_by_dates(start_date, end_date, output_directory: str = output_dir):
    """Process single date range.

    This function fetches samples from the EBI API for a given date range
    and writes them to a Parquet file. A semaphore file is created to indicate
    that the process is complete for the given date range.
    """
    fetcher = SampleFetcher(
        cursor="*",
        size=200,
        start_date=start_date,
        end_date=end_date,
        output_directory=output_directory,
    )
    await fetcher.process()

    tmp_filename = get_filename(start_date, end_date, tmp=True, output_directory=output_directory)
    final_filename = get_filename(start_date, end_date, tmp=False, output_directory=output_directory)

    if fetcher.any_samples:
        # Write samples to Parquet file
        schema = get_biosample_schema()
        table = pa.Table.from_pylist(fetcher.samples_buffer, schema=schema)
        pq.write_table(
            table,
            tmp_filename,
            compression="zstd",
            compression_level=9
        )

        # Move temp file to final location
        shutil.move(tmp_filename, final_filename)
        # Create .done file next to the data file
        UPath(final_filename + ".done").touch()
        logger.info(f"Finished processing {start_date} to {end_date}: {fetcher.processed_count} samples extracted")
    else:
        # No samples found - create .done with special marker
        # Create .done file to mark day as processed (even though no data)
        # This prevents re-checking empty days
        done_file = UPath(final_filename + ".done")
        done_file.touch()
        # Write metadata to indicate no samples
        done_file.write_text("NO_SAMPLES\n")
        logger.info(f"Finished processing {start_date} to {end_date}: No samples found")
    UPath(tmp_filename).unlink(missing_ok=True)
    


async def limited_process(semaphore, start_date, end_date, output_directory: str = output_dir):
    """This function is a wrapper around process_by_dates that limits the number of concurrent tasks."""
    async with semaphore:
        await process_by_dates(start_date, end_date, output_directory)


async def main(output_directory: str = output_dir):
    start = "2021-01-01"
    # Extract up to yesterday to avoid partial day data
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    end = yesterday
    semaphore = anyio.Semaphore(20)  # Limit to 20 concurrent tasks

    logger.info(f"Starting EBI Biosample extraction from {start} to {end}")
    logger.info(f"Extracting up to yesterday to ensure complete days")
    logger.info(f"Output directory: {output_directory}")

    async with anyio.create_task_group() as task_group:
        for start_date, end_date in get_date_ranges(start, end):
            if not UPath(
                get_filename(start_date, end_date, tmp=False, output_directory=output_directory) + ".done"
            ).exists(): # and current_date < end_date: # repeat current month
                logger.info(f"Scheduling processing for {start_date} to {end_date}")
                task_group.start_soon(limited_process, semaphore, start_date, end_date, output_directory)


@click.group()
def ebi_biosample():
    pass

@ebi_biosample.command()
@click.option(
    "--output-dir",
    type=click.Path(),
    default=None,
    help=f"Output directory for extracted data (default: {output_dir})",
)
def extract(output_dir: str):
    """Extract EBI Biosample data.

    Fetches biosample data from EBI API and saves to NDJSON format,
    organized by monthly date ranges.
    """
    if output_dir is None:
        output_dir = str(UPath(settings.PUBLISH_DIRECTORY) / "ebi_biosample")

    logger.info(f"Using output directory: {output_dir}")
    anyio.run(main, output_dir)


if __name__ == "__main__":
    logger.info("Starting EBI Biosample extraction")
    anyio.run(main)
