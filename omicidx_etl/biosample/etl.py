from omicidx.biosample import BioSampleParser, BioProjectParser
import tempfile
import gzip
import orjson
from upath import UPath
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


@task
def biosample_parse(url: str):
    with tempfile.NamedTemporaryFile() as tmpfile:
        urllib.request.urlretrieve(url, tmpfile.name)
        tmpfile.seek(0)

        # clean up old output files
        for f in UPath(OUTPUT_DIR).glob("biosample*.gz"):
            f.unlink()

        obj_counter = 0
        file_counter = 0
        max_lines_per_file = 100000

        outfile_path = UPath(OUTPUT_DIR) / f"biosample-{file_counter:06}.ndjson.gz"
        outfile = UPath(outfile_path).open("wb", compression="gzip")

        with gzip.open(tmpfile.name, "rb") as fh:
            for obj in BioSampleParser(fh, validate_with_schema=False):  # type: ignore
                if obj_counter >= max_lines_per_file:
                    outfile.close()
                    file_counter += 1
                    outfile_path = (
                        UPath(OUTPUT_DIR) / f"biosample-{file_counter:06}.ndjson.gz"
                    )
                    outfile = UPath(outfile_path).open("wb", compression="gzip")
                    obj_counter = 0

                outfile.write(orjson.dumps(obj) + b"\n")
                obj_counter += 1

        outfile.close()


@task
def bioproject_parse(url: str):
    with tempfile.NamedTemporaryFile() as tmpfile:
        urllib.request.urlretrieve(url, tmpfile.name)
        tmpfile.seek(0)

        # clean up old output files
        for f in UPath(OUTPUT_DIR).glob("bioproject*.gz"):
            f.unlink()

        obj_counter = 0
        file_counter = 0
        max_lines_per_file = 100000

        outfile_path = UPath(OUTPUT_DIR) / f"bioproject-{file_counter:06}.ndjson.gz"
        outfile = UPath(outfile_path).open("wb", compression="gzip")

        with open(tmpfile.name, "rb") as fh:
            for obj in BioProjectParser(fh, validate_with_schema=False):  # type: ignore
                if obj_counter >= max_lines_per_file:
                    outfile.close()
                    file_counter += 1
                    outfile_path = (
                        UPath(OUTPUT_DIR) / f"bioproject-{file_counter:06}.ndjson.gz"
                    )
                    outfile = UPath(outfile_path).open("wb", compression="gzip")
                    obj_counter = 0

                outfile.write(orjson.dumps(obj) + b"\n")
                obj_counter += 1

        outfile.close()


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
    load_bioentities_to_bigquery("biosample", "biosamples")
    load_bioentities_to_bigquery("bioproject", "bioprojects")


if __name__ == "__main__":
    process_biosamaple_and_bioproject()
