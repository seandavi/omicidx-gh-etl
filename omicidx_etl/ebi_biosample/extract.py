import asyncio
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from typing import Iterable
import tenacity
import anyio
import httpx
import orjson
from upath import UPath
import shutil
import gzip
from ..config import settings
import click
from loguru import logger


output_dir = str(UPath(settings.PUBLISH_DIRECTORY) / "ebi_biosample")


def get_filename(
    start_date: date,
    end_date: date,
    tmp: bool = True,
    output_directory: str = output_dir
) -> str:
    """Get the filename for a given date range.

    Currently, this is a GCS path. The final filename looks like
    `biosamples-2021-01-01--2021-01-31.ndjson.gz`, for example.
    """
    # base = f"{bucket}/{path}/biosamples-{start_date.strftime('%Y-%m-%d')}--{end_date.strftime('%Y-%m-%d')}--daily.ndjson.gz"
    base = f"{output_directory}/biosamples-{start_date.strftime('%Y-%m-%d')}--{end_date.strftime('%Y-%m-%d')}--daily.ndjson.gz"
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
        self.upath = UPath(settings.PUBLISH_DIRECTORY) / get_filename(
            start_date, end_date, tmp=True, output_directory=output_directory
        )
        self.fh = gzip.open(get_filename(start_date, end_date, tmp=True, output_directory=output_directory), "wb")

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
            except KeyError:
                pass

    async def process(self):
        """Process the samples from the EBI API.

        This function fetches samples from the EBI API and writes them
        to a file. It runs in a loop until there are no more samples
        to fetch.
        """
        metadata = {"record_count": 0}

        async for sample in self.fetch_next_set():
            self.fh.write(orjson.dumps(sample) + b"\n")
            metadata["record_count"] += 1
            if metadata["record_count"] % 10000 == 0:
                logger.info(f"Fetched {metadata['record_count']} samples so far")

    def completed(self):
        """Finalize the fetching process.

        This function is called when there are no more samples to fetch.
        It is mainly here to close the file handle.
        """
        logger.info("Completed fetching samples")
        self.fh.close()
        logger.info(self)
        pass


def get_date_ranges(start_date_str: str, end_date_str: str) -> Iterable[tuple]:
    """Get date ranges for a given start and end date.

    Given a start and end date, returns a list of tuples representing the start and end dates of each month in the range.

    :param start_date_str: The start date in 'YYYY-MM-DD' format
    :param end_date_str: The end date in 'YYYY-MM-DD' format
    :return: List of tuples, each containing the start and end date of a month in the range
    """
    # Convert strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    date_ranges = []
    current_start = start_date.replace(day=1)

    while current_start <= end_date:
        # Calculate the end of the current month
        current_end = (current_start + relativedelta(months=1)) - timedelta(days=1)

        date_ranges.append((current_start.date(), current_end.date()))
        yield (current_start.date(), current_end.date())
        # Move to the first day of the next month
        current_start = current_start + relativedelta(months=1)


async def process_by_dates(start_date, end_date, output_directory: str = output_dir):
    """Process single date range.

    This function fetches samples from the EBI API for a given date range
    and writes them to a file. The file is then moved to its final location.
    A semaphore file is created to indicate that the process is complete
    for the given date range.
    """
    fetcher = SampleFetcher(
        cursor="*",
        size=200,
        start_date=start_date,
        end_date=end_date,
        output_directory=output_directory,
    )
    await fetcher.process()
    if fetcher.any_samples:
        shutil.move(
            get_filename(start_date, end_date, tmp=True, output_directory=output_directory),
            get_filename(start_date, end_date, tmp=False, output_directory=output_directory),
        )
    else:
        import os

        os.unlink(get_filename(start_date, end_date, tmp=True, output_directory=output_directory))
    # touch filename with .done extension
    UPath(get_filename(start_date, end_date, tmp=False, output_directory=output_directory) + ".done").touch()
    logger.info(f"Finished processing {start_date} to {end_date}")


async def limited_process(semaphore, start_date, end_date, output_directory: str = output_dir):
    """This function is a wrapper around process_by_dates that limits the number of concurrent tasks."""
    async with semaphore:
        await process_by_dates(start_date, end_date, output_directory)


async def main(output_directory: str = output_dir):
    start = "2021-01-01"
    end = datetime.now().strftime("%Y-%m-%d")
    current_date = datetime.now().date()
    semaphore = anyio.Semaphore(20)  # Limit to 10 concurrent tasks

    logger.info(f"Starting EBI Biosample extraction from {start} to {end}")
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
    asyncio.run(main(output_dir))


if __name__ == "__main__":
    logger.info("Starting EBI Biosample extraction")
    asyncio.run(main())
