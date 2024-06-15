from omicidx.biosample import BioSampleParser, BioProjectParser
import tempfile
import gzip
import orjson
from upath import UPath
import urllib.request

from ...logging import get_logger

logger = get_logger(__name__)

BIO_SAMPLE_URL = "https://ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz"
BIO_PROJECT_URL = "https://ftp.ncbi.nlm.nih.gov/bioproject/bioproject.xml"
OUTPUT_DIR = "gs://omicidx-json/biosample"


def biosample_parse(url: str, outfile_name: str):
    with tempfile.NamedTemporaryFile() as tmpfile:
        urllib.request.urlretrieve(url, tmpfile.name)
        tmpfile.seek(0)
        with gzip.open(tmpfile.name, "rb") as fh:
            with UPath(outfile_name).open("wb", compression="gzip") as outfile:
                for obj in BioSampleParser(fh, validate_with_schema=False):  # type: ignore
                    outfile.write(orjson.dumps(obj) + b"\n")


def bioproject_parse(url: str, outfile_name: str):
    with tempfile.NamedTemporaryFile() as tmpfile:
        urllib.request.urlretrieve(url, tmpfile.name)
        tmpfile.seek(0)
        with open(tmpfile.name, "rb") as fh:
            with UPath(outfile_name).open("wb", compression="gzip") as outfile:
                for obj in BioProjectParser(fh, validate_with_schema=False):
                    outfile.write(orjson.dumps(obj) + b"\n")


def biosample_get_urls():
    logger.info("Parsing BioProject and BioSample")
    logger.info(f"BioProject URL: {BIO_PROJECT_URL}")
    bioproject_parse(
        url=BIO_PROJECT_URL,
        outfile_name=f"{OUTPUT_DIR}/bioproject.ndjson.gz",
    )
    logger.info(f"BioSample output to gs://omicidx-json/biosample/bioproject.ndjson.gz")
    logger.info(f"BioSample URL: {BIO_SAMPLE_URL}")
    biosample_parse(
        url=BIO_SAMPLE_URL,
        outfile_name=f"{OUTPUT_DIR}/biosample.ndjson.gz",
    )
    logger.info("BioSample output to gs://omicidx-json/biosample/biosample.ndjson.gz")
    logger.info("Done")


if __name__ == "__main__":
    biosample_get_urls()
