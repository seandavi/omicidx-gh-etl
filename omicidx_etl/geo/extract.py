import anyio
import re
import faulthandler
from upath import UPath
from datetime import timedelta, datetime, date
from dateutil.relativedelta import relativedelta
import click
import orjson

from ..config import settings

import gzip

import httpx
from anyio import create_memory_object_stream
from anyio.streams.memory import MemoryObjectSendStream, MemoryObjectReceiveStream
from omicidx.geo import parser as gp
from tenacity import retry
import tenacity
from loguru import logger


import tempfile
import shutil

OUTPUT_PATH = UPath(settings.PUBLISH_DIRECTORY) / "geo"
OUTPUT_DIR = str(OUTPUT_PATH)

faulthandler.enable()


@retry(
    wait=tenacity.wait_fixed(2),
    stop=tenacity.stop_after_attempt(5),
    retry=tenacity.retry_if_exception(
        lambda e: (
            (isinstance(e, httpx.HTTPStatusError) and e.response.status_code == 429)
            or isinstance(
                e,
                (httpx.RemoteProtocolError, httpx.ConnectError, httpx.TimeoutException),
            )
        )
    ),
    before_sleep=lambda retry_state: logger.warning(
        f"GEO SOFT request failed, retrying in 2 seconds (attempt {retry_state.attempt_number}/5)"
    ),
)
async def get_geo_soft(accession, client) -> str:
    """Fetches the GEO SOFT file for the given accession."""
    params = {}
    params['acc'] = accession
    params['targ'] = 'self'
    params['form'] = 'text'
    params['view'] = 'quick' if accession.startswith("GSM") else 'brief'
    url = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi"
    response = await client.get(url, params=params)
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


def get_result_paths(start_date, end_date, output_path):
    basepath = output_path
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
    output_path: UPath = OUTPUT_PATH,
):
    """Writes the entity to a file."""
    gse_path, gsm_path, gpl_path = get_result_paths(start_date, end_date, output_path)

    record_counts = {
        "GSE": 0,
        "GSM": 0,
        "GPL": 0,
    }

    with (
        tempfile.NamedTemporaryFile() as gse_temp,
        tempfile.NamedTemporaryFile() as gsm_temp,
        tempfile.NamedTemporaryFile() as gpl_temp,
    ):
        gse_written = False
        gsm_written = False
        gpl_written = False

        gse_tmp_write = gzip.open(gse_temp.name, "wb")
        gsm_tmp_write = gzip.open(gsm_temp.name, "wb")
        gpl_tmp_write = gzip.open(gpl_temp.name, "wb")

        async with entity_text_to_process_receive:
            async for text in entity_text_to_process_receive:
                lines = [x.strip() for x in text.split("\n")]
                entity = gp._parse_single_entity_soft(lines)
                if entity is None:
                    continue
                if entity.accession.startswith("GSE"):  # type: ignore
                    gse_tmp_write.write(
                        entity.model_dump_json().encode("utf-8") + b"\n"
                    )  # type: ignore
                    gse_written = True
                elif entity.accession.startswith("GSM"):  # type: ignore
                    gsm_tmp_write.write(
                        entity.model_dump_json().encode("utf-8") + b"\n"
                    )  # type: ignore
                    gsm_written = True
                elif entity.accession.startswith("GPL"):  # type: ignore
                    gpl_tmp_write.write(
                        entity.model_dump_json().encode("utf-8") + b"\n"
                    )  # type: ignore
                    gpl_written = True
                record_counts[entity.accession[:3]] += 1  # type: ignore

        gse_tmp_write.close()
        gsm_tmp_write.close()
        gpl_tmp_write.close()

        # Copy temporary files to final destinations
        if gse_written:
            with open(gse_temp.name, "rb") as src:
                with gse_path.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
            logger.info(f"Wrote {gse_path}")

        if gsm_written:
            with open(gsm_temp.name, "rb") as src:
                with gsm_path.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
            logger.info(f"Wrote {gsm_path}")

        if gpl_written:
            with open(gpl_temp.name, "rb") as src:
                with gpl_path.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
            logger.info(f"Wrote {gpl_path}")

    logger.info(f"Record counts: {record_counts}")


def entrezid_to_geo(entrezid: str):
    if entrezid.startswith("2"):
        return re.sub("^20*", "GSE", entrezid)
    elif entrezid.startswith("1"):
        return re.sub("^10*", "GPL", entrezid)
    elif entrezid.startswith("3"):
        return re.sub("^30*", "GSM", entrezid)

    raise ValueError("Expected entrezid to start with 1, 2, or 3")


@retry(
    wait=tenacity.wait_fixed(2),
    stop=tenacity.stop_after_attempt(5),
    retry=tenacity.retry_if_exception(
        lambda e: (
            (isinstance(e, httpx.HTTPStatusError) and e.response.status_code == 429)
            or isinstance(
                e,
                (httpx.RemoteProtocolError, httpx.ConnectError, httpx.TimeoutException),
            )
        )
    ),
    before_sleep=lambda retry_state: logger.warning(
        f"Entrez API request failed, retrying in 2 seconds (attempt {retry_state.attempt_number}/5)"
    ),
)
async def prod1(accessions_to_fetch_send: MemoryObjectSendStream, start_date, end_date):
    offset = 0
    RETMAX = 5000
    async with accessions_to_fetch_send:
        while True:
            async with httpx.AsyncClient(timeout=60) as client:
                logger.debug(f"Fetching {start_date} to {end_date} offset {offset}")
                response = await client.get(
                    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
                    params={
                        "db": "gds",
                        "term": f"""(GSM[etyp] OR GSE[etyp] OR GPL[etyp]) AND ("{start_date.strftime("%Y/%m/%d")}"[Update Date] : "{end_date.strftime("%Y/%m/%d")}"[Update Date])""",
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

@tenacity.retry(
    wait=tenacity.wait_fixed(2),
    stop=tenacity.stop_after_attempt(5),
    retry=tenacity.retry_if_exception(
        lambda e: (
            (isinstance(e, httpx.HTTPStatusError) and e.response.status_code == 429)
            or isinstance(
                e,
                (httpx.RemoteProtocolError, httpx.ConnectError, httpx.TimeoutException),
            )
        )
    ),
    before_sleep=lambda retry_state: logger.warning(
        f"Entrez API request failed, retrying in 2 seconds (attempt {retry_state.attempt_number}/5)"
    ),
)
def gse_with_rna_seq_counts() -> list[dict[str, str]]:
    """GEO supplies a hidden filter for getting GSEs with RNA-seq counts
    
    The filter is at the level of GSEs, not GSMs. This function just 
    applies the filter and returns a list of GSEs that have GEO/SRA-supplied
    RNA-seq counts.
    
    It is very fast to run since it runs against eutils and only returns
    ids. 
    """
    offset = 0
    RETMAX = 5000
    gses_with_rna_seq_counts = []
    while True:
        with httpx.Client(timeout=60) as client:
            response = client.get(
                "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
                params={
                    "db": "gds",
                    "term": '"rnaseq+counts"[filter]',
                    "retmode": "json",
                    "retmax": RETMAX,
                    "retstart": offset,
                },
            )
            response.raise_for_status()
            json_results = response.json()
            for id in json_results["esearchresult"]["idlist"]:
                gses_with_rna_seq_counts.append(
                    {"accession": entrezid_to_geo(id)}
                )
            if len(json_results["esearchresult"]["idlist"]) < RETMAX:
                break
            offset += 5000
            import time
            time.sleep(0.5)  # to avoid hitting the rate limit
    # dict of {"accession": <GSE_ACCESSION>}
    # ready for polars.from_pylist() or writing out as json.
    return gses_with_rna_seq_counts



async def geo_metadata_by_date(
    start_date: date = date(2000, 1, 1),
    end_date: date = date.today(),
    output_path: UPath = OUTPUT_PATH,
):
    gse_path, gsm_path, gpl_path = get_result_paths(start_date, end_date, output_path)
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
                output_path,
            )
        async with accessions_to_fetch_send:
            tg.start_soon(prod1, accessions_to_fetch_send.clone(), start_date, end_date)


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
        # if current_end > end_date:
        #    current_end = end_date
        monthly_ranges.append((current_start.date(), current_end.date()))
        # Move to the first day of the next month
        current_start = current_start + relativedelta(months=1)

    return monthly_ranges


async def main():
    # Get the GSEs with RNA-seq counts
    # updated each run since it is very fast
    
    import os
    print(os.environ)
    
    gses_with_rna_seq = gse_with_rna_seq_counts()
    with gzip.open(OUTPUT_PATH / "gse_with_rna_seq_counts.jsonl.gz", "wb") as f:
        for item in gses_with_rna_seq:
            f.write(orjson.dumps(item) + b"\n")
    logger.info(f"Wrote {len(gses_with_rna_seq)} GSEs with RNA-seq counts to {OUTPUT_PATH / 'gse_with_rna_seq_counts.jsonl.gz'}")
    start = "2005-01-01"
    end = date.today().strftime("%Y-%m-%d")
    ranges = get_monthly_ranges(start, end)
    for start_date, end_date in ranges:
        logger.info(f"Processing GEO metadata from {start_date} to {end_date}")
        await geo_metadata_by_date(start_date, end_date, OUTPUT_PATH)


@click.group()
def geo():
    """OmicIDX ETL Pipeline - GEO data extraction tools."""
    pass


@geo.command()
def extract():
    """Extract GEO metadata."""
    anyio.run(main)
