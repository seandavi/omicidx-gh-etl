from omicidx.biosample import BioSampleParser, BioProjectParser
import tempfile
import gzip
import orjson
from upath import UPath
import urllib.request
from google.cloud import bigquery
from prefect import task, flow

from ..logging import get_logger

logger = get_logger(__name__)

BIO_SAMPLE_URL = "https://ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz"
BIO_PROJECT_URL = "https://ftp.ncbi.nlm.nih.gov/bioproject/bioproject.xml"
OUTPUT_DIR = "gs://omicidx-json/biosample"


@task(task_run_name="load-{entity}-to-bigquery")
def load_bioentities_to_bigquery(entity: str, plural_entity: str):
    """
    Load biosample or bioproject to BigQuery.

    Args:
        entity (str): The entity to load.
        plural_entity (str): The plural form of the entity.
    """

    client = bigquery.Client()
    load_job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    uri = f"gs://omicidx-json/biosample/{entity}.ndjson.gz"
    dataset = "biodatalake"
    table = f"src_ncbi__{plural_entity}"
    job = client.load_table_from_uri(
        uri, f"{dataset}.{table}", job_config=load_job_config
    )

    return job.result()  # Waits for the job to complete.


@task
def biosample_parse(url: str, outfile_name: str):
    with tempfile.NamedTemporaryFile() as tmpfile:
        urllib.request.urlretrieve(url, tmpfile.name)
        tmpfile.seek(0)
        with gzip.open(tmpfile.name, "rb") as fh:
            with UPath(outfile_name).open("wb", compression="gzip") as outfile:
                for obj in BioSampleParser(fh, validate_with_schema=False):  # type: ignore
                    outfile.write(orjson.dumps(obj) + b"\n")


@task
def bioproject_parse(url: str, outfile_name: str):
    with tempfile.NamedTemporaryFile() as tmpfile:
        urllib.request.urlretrieve(url, tmpfile.name)
        tmpfile.seek(0)
        with open(tmpfile.name, "rb") as fh:
            with UPath(outfile_name).open("wb", compression="gzip") as outfile:
                for obj in BioProjectParser(fh, validate_with_schema=False):
                    outfile.write(orjson.dumps(obj) + b"\n")


@flow
def process_biosamaple_and_bioproject():
    logger.info("Parsing BioProject and BioSample")
    logger.info(f"BioProject URL: {BIO_PROJECT_URL}")
    bioproject_parse(
        url=BIO_PROJECT_URL,
        outfile_name=f"{OUTPUT_DIR}/bioproject.ndjson.gz",
    )
    logger.info("BioSample output to gs://omicidx-json/biosample/bioproject.ndjson.gz")
    logger.info(f"BioSample URL: {BIO_SAMPLE_URL}")
    load_bioentities_to_bigquery("bioproject", "bioprojects")
    biosample_parse(
        url=BIO_SAMPLE_URL,
        outfile_name=f"{OUTPUT_DIR}/biosample.ndjson.gz",
    )
    logger.info("BioSample output to gs://omicidx-json/biosample/biosample.ndjson.gz")
    logger.info("Done")
    logger.info("Loading BioSample to BigQuery")
    load_bioentities_to_bigquery("biosample", "biosamples")


if __name__ == "__main__":
    process_biosamaple_and_bioproject()
