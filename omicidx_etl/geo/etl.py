import anyio
import logging
import re
import faulthandler
from upath import UPath
from datetime import timedelta, datetime, date
from dateutil.relativedelta import relativedelta
from prefect import task, flow
from ..config import settings

import httpx
import orjson
from anyio import create_memory_object_stream
from anyio.streams.memory import MemoryObjectSendStream, MemoryObjectReceiveStream
from omicidx.geo import parser as gp
from tenacity import retry
import tenacity

from .load import load_to_bigquery

logging.basicConfig(level=logging.INFO)

OUTPUT_PATH = UPath(settings.PUBLISH_DIRECTORY) / "geo"
OUTPUT_DIR = str(OUTPUT_PATH)


def get_run_logger():
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger = logging.getLogger("geo")
    logger.setLevel(logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logger.addHandler(stream_handler)
    return logger


faulthandler.enable()


@retry(wait=tenacity.wait_fixed(2), stop=tenacity.stop_after_attempt(5))
async def get_geo_soft(accession, client) -> str:
    """Fetches the GEO SOFT file for the given accession."""
    url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?targ=self&acc={accession}&form=text&view=brief"
    response = await client.get(url)
    response.raise_for_status()
    return response.text


async def fetch_geo_soft_worker(
    accessions_to_fetch_receive: MemoryObjectReceiveStream,  # from entrez search
    entity_text_to_process_send: MemoryObjectSendStream,  # to process_entitity_worker
):
    """Fetches the GEO SOFT files for the accessions.

    We read from receive stream and send the text to the send stream.
    The send stream is then processed by the write_geo_ids function.
    """
    async with httpx.AsyncClient(timeout=30) as client:
        async with accessions_to_fetch_receive, entity_text_to_process_send:
            async for accession in accessions_to_fetch_receive:
                geo_text = await get_geo_soft(accession, client)
                await entity_text_to_process_send.send(geo_text)


async def get_result_paths(start_date, end_date):
    basepath = OUTPUT_PATH
    gse_path = (
        basepath
        / f"gse-{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}.ndjson.gz"
    )
    gsm_path = (
        basepath
        / f"gsm-{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}.ndjson.gz"
    )
    gpl_path = (
        basepath
        / f"gpl-{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}.ndjson.gz"
    )
    return gse_path, gsm_path, gpl_path


async def write_geo_entity_worker(
    entity_text_to_process_receive: MemoryObjectReceiveStream,  # from process_entitity_worker
    start_date: date = date(2000, 1, 1),
    end_date: date = date.today(),
):
    """Writes the entity to a file."""
    gse_path, gsm_path, gpl_path = await get_result_paths(start_date, end_date)

    gse_f = None
    gsm_f = None
    gpl_f = None
    async with entity_text_to_process_receive:
        async for text in entity_text_to_process_receive:
            lines = [x.strip() for x in text.split("\n")]
            entity = gp._parse_single_entity_soft(lines)
            if entity is None:
                continue
            if entity.accession.startswith("GSE"):  # type: ignore
                if gse_f is None:
                    gse_f = gse_path.open("wb", compression="gzip")
                gse_f.write(orjson.dumps(entity.dict()) + b"\n")  # type: ignore
            elif entity.accession.startswith("GSM"):  # type: ignore
                if gsm_f is None:
                    gsm_f = gsm_path.open("wb", compression="gzip")
                gsm_f.write(orjson.dumps(entity.dict()) + b"\n")  # type: ignore
            elif entity.accession.startswith("GPL"):  # type: ignore
                if gpl_f is None:
                    gpl_f = gpl_path.open("wb", compression="gzip")
                gpl_f.write(orjson.dumps(entity.dict()) + b"\n")  # type: ignore
    if gse_f is not None:
        gse_f.close()
    if gsm_f is not None:
        gsm_f.close()
    if gpl_f is not None:
        gpl_f.close()
    print("exiting from write_geo_entity_worker")


def entrezid_to_geo(entrezid: str):
    if entrezid.startswith("2"):
        return re.sub("^20*", "GSE", entrezid)
    elif entrezid.startswith("1"):
        return re.sub("^10*", "GPL", entrezid)
    elif entrezid.startswith("3"):
        return re.sub("^30*", "GSM", entrezid)

    raise ValueError("Expected entrezid to start with 1, 2, or 3")


async def prod1(accessions_to_fetch_send: MemoryObjectSendStream, start_date, end_date):
    offset = 0
    RETMAX = 5000
    logger = get_run_logger()
    async with accessions_to_fetch_send:
        while True:
            async with httpx.AsyncClient(timeout=60) as client:
                logger.debug(f"Fetching {start_date} to {end_date} offset {offset}")
                response = await client.get(
                    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
                    params={
                        "db": "gds",
                        "term": f"""(GSM[etyp] OR GSE[etyp] OR GPL[etyp]) AND ("{start_date.strftime('%Y/%m/%d')}"[Update Date] : "{end_date.strftime('%Y/%m/%d')}"[Update Date])""",
                        "retmode": "json",
                        "retmax": RETMAX,
                        "retstart": offset,
                    },
                )
                response.raise_for_status()
                json_results = response.json()
                for id in json_results["esearchresult"]["idlist"]:
                    await accessions_to_fetch_send.send(entrezid_to_geo(id))
                if len(json_results["esearchresult"]["idlist"]) < RETMAX:
                    break
                offset += 5000


@task(task_run_name="metadata-by-date--{start_date}-{end_date}")
async def geo_metadata_by_date(
    start_date: date = date(2000, 1, 1),
    end_date: date = date.today(),
):
    logger = get_run_logger()
    gse_path, gsm_path, gpl_path = await get_result_paths(start_date, end_date)
    if (
        gse_path.exists() or gsm_path.exists() or gpl_path.exists()
    ) and end_date < date.today():
        logger.debug(f"Skipping {start_date} to {end_date} since it already exists")
        return
    (
        accessions_to_fetch_send,
        accessions_to_fetch_receive,
    ) = create_memory_object_stream(100)
    (
        entity_text_to_process_send,
        entity_text_to_process_receive,
    ) = create_memory_object_stream(100)

    async with anyio.create_task_group() as tg:
        # start 30 workers to fetch the GEO SOFT files
        async with accessions_to_fetch_receive, entity_text_to_process_send:
            for i in range(30):
                tg.start_soon(
                    fetch_geo_soft_worker,
                    accessions_to_fetch_receive.clone(),
                    entity_text_to_process_send.clone(),
                )
        # start a worker to write the entity to a file
        # this worker will write the entity to a file
        # one for each of GSE, GSM, and GPL
        async with entity_text_to_process_receive:
            tg.start_soon(
                write_geo_entity_worker,
                entity_text_to_process_receive.clone(),
                start_date,
                end_date,
            )
        async with accessions_to_fetch_send:
            tg.start_soon(prod1, accessions_to_fetch_send.clone(), start_date, end_date)


@task
def get_monthly_ranges(start_date_str: str, end_date_str: str) -> list[tuple]:
    """
    Given a start and end date, returns a list of tuples representing the start and end dates of each month in the range.

    :param start_date_str: The start date in 'YYYY-MM-DD' format
    :param end_date_str: The end date in 'YYYY-MM-DD' format
    :return: List of tuples, each containing the start and end date of a month in the range
    """
    # Convert strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    monthly_ranges = []
    current_start = start_date.replace(day=1)

    while current_start <= end_date:
        # Calculate the end of the current month
        current_end = (current_start + relativedelta(months=1)) - timedelta(days=1)
        # Adjust the end date if it's beyond the given end_date
        if current_end > end_date:
            current_end = end_date
        monthly_ranges.append((current_start.date(), current_end.date()))
        # Move to the first day of the next month
        current_start = current_start + relativedelta(months=1)

    return monthly_ranges


@task
def task_load_to_bigquery(entity: str):
    return load_to_bigquery(entity)


@flow
async def main():
    logger = get_run_logger()
    start = "2005-01-01"
    end = date.today().strftime("%Y-%m-%d")
    ranges = get_monthly_ranges(start, end)
    for start_date, end_date in ranges:
        await geo_metadata_by_date(start_date, end_date)

    # for later
    # for entity in ["gse", "gsm", "gpl"]:
    #     job_result = task_load_to_bigquery(entity)
    #     logger.info(f"Loaded {entity} to BigQuery")
    #     logger.info(str(job_result))


if __name__ == "__main__":
    anyio.run(main)
