"""
DEPRECATED: This module contains the original Prefect-based ETL code.

For new development, use the simplified extraction functions in extract.py:
    from omicidx_etl.biosample.extract import extract_all, extract_biosample, extract_bioproject

Or use the CLI:
    python -m omicidx_etl.cli biosample extract

This file is kept for reference and compatibility during migration.
"""

from omicidx.biosample import BioSampleParser, BioProjectParser
import tempfile
import gzip
import orjson
from upath import UPath
import shutil
import urllib.request
from google.cloud import bigquery
from prefect import task, flow
from ..config import settings

from ..logging import get_logger
from .schema import get_schema

logger = get_logger(__name__)

BIO_SAMPLE_URL = "https://ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz"
BIO_PROJECT_URL = "https://ftp.ncbi.nlm.nih.gov/bioproject/bioproject.xml"
OUTPUT_PATH = UPath(settings.PUBLISH_DIRECTORY) / "biosample"
OUTPUT_DIR = str(OUTPUT_PATH)


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
        schema=get_schema(entity),
        write_disposition="WRITE_TRUNCATE",
    )

    uri = f"gs://omicidx/biosample/{entity}-*.ndjson.gz"
    dataset = "omicidx"
    table = f"src_ncbi__{plural_entity}"
    job = client.load_table_from_uri(
        uri, f"{dataset}.{table}", job_config=load_job_config
    )

    return job.result()  # Waits for the job to complete.


def cleanup_old_output_files(entity: str):
    for f in UPath(OUTPUT_DIR).glob(f"{entity}*.gz"):
        f.unlink()

@task
def biosample_parse(url: str):
    """Parse the biosample XML file and write to gzipped ndjson files."""
    cleanup_old_output_files("biosample")
    
    with tempfile.NamedTemporaryFile() as tmpfile:
        urllib.request.urlretrieve(url, tmpfile.name)
        tmpfile.seek(0)


        obj_counter = 0
        file_counter = 0
        max_lines_per_file = 1000000 # 10x more that bioproject since biosample records are smaller.

        # Create local temporary file for writing
        local_temp = tempfile.NamedTemporaryFile(delete=False)
        outfile = gzip.open(local_temp.name, "wb")

        def _copy_and_reset():
            nonlocal outfile, file_counter, obj_counter
            outfile.close()
            outfile_path = UPath(OUTPUT_DIR) / f"biosample-{file_counter:06}.ndjson.gz"
            with open(local_temp.name, "rb") as src:
                with outfile_path.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
            file_counter += 1
            obj_counter = 0
            outfile = gzip.open(local_temp.name, "wb")

        with gzip.open(tmpfile.name, "rb") as fh:
            for obj in BioSampleParser(fh, validate_with_schema=False):  # type: ignore
                if obj_counter >= max_lines_per_file:
                    _copy_and_reset()

                outfile.write(orjson.dumps(obj) + b"\n")
                obj_counter += 1

        # Copy final file
        outfile.close()
        outfile_path = UPath(OUTPUT_DIR) / f"biosample-{file_counter:06}.ndjson.gz"
        with open(local_temp.name, "rb") as src:
            with outfile_path.open("wb") as dst:
                shutil.copyfileobj(src, dst)
        
        # Clean up local temp file
        UPath(local_temp.name).unlink()


@task
def bioproject_parse(url: str):

    cleanup_old_output_files("bioproject")

    with tempfile.NamedTemporaryFile() as tmpfile:
        urllib.request.urlretrieve(url, tmpfile.name)
        tmpfile.seek(0)

        obj_counter = 0
        file_counter = 0
        max_lines_per_file = 100000

        # Create local temporary file for writing
        local_temp = tempfile.NamedTemporaryFile(delete=False)
        outfile = gzip.open(local_temp.name, "wb")

        def _copy_and_reset():
            nonlocal outfile, file_counter, obj_counter
            outfile.close()
            outfile_path = UPath(OUTPUT_DIR) / f"bioproject-{file_counter:06}.ndjson.gz"
            with open(local_temp.name, "rb") as src:
                with outfile_path.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
            file_counter += 1
            obj_counter = 0
            outfile = gzip.open(local_temp.name, "wb")

        with open(tmpfile.name, "rb") as fh:
            for obj in BioProjectParser(fh, validate_with_schema=False):  # type: ignore
                if obj_counter >= max_lines_per_file:
                    _copy_and_reset()

                outfile.write(orjson.dumps(obj) + b"\n")
                obj_counter += 1

        # Copy final file
        outfile.close()
        outfile_path = UPath(OUTPUT_DIR) / f"bioproject-{file_counter:06}.ndjson.gz"
        with open(local_temp.name, "rb") as src:
            with outfile_path.open("wb") as dst:
                shutil.copyfileobj(src, dst)
                
        # Clean up local temp file
        UPath(local_temp.name).unlink()


@flow
def process_biosamaple_and_bioproject():
    logger.info("Parsing BioProject and BioSample")
    logger.info(f"BioProject URL: {BIO_PROJECT_URL}")
    bioproject_parse(
        url=BIO_PROJECT_URL,
    )
    logger.info("BioSample output to gs://omicidx-json/biosample/bioproject.ndjson.gz")
    logger.info(f"BioSample URL: {BIO_SAMPLE_URL}")
    # load_bioentities_to_bigquery("bioproject", "bioprojects")
    biosample_parse(
        url=BIO_SAMPLE_URL,
    )
    logger.info("BioSample output to gs://omicidx-json/biosample/biosample.ndjson.gz")
    logger.info("Done")
    logger.info("Loading BioProject and BioSample to BigQuery")
    #load_bioentities_to_bigquery("biosample", "biosamples")
    #load_bioentities_to_bigquery("bioproject", "bioprojects")


if __name__ == "__main__":
    process_biosamaple_and_bioproject()
