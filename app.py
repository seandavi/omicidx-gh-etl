from omicidx.sra.parser import sra_object_generator
from upath import UPath

import orjson
import gzip
import tempfile
import shutil

import re

import logging
from rich.logging import RichHandler

logging.basicConfig(
    level="INFO",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)],
)

logging.basicConfig(level=logging.INFO)


def mirror_dirlist_for_current_month(current_month_only: bool = False) -> list[UPath]:
    """return a list of UPath objects for the current month's mirror directories

    The NCBI SRA mirror directory is organized by date. This function finds the
    most recent full mirror directory for the current month and all directories of
    incremental updates for the current month.

    Returns:
        list[UPath]: a list of UPath objects

    >>> mirror_dirlist_for_current_month()
    """
    u = UPath("https://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring")
    pathlist = sorted(list(u.glob("**/")), reverse=True)
    index = 0
    for path in pathlist:
        index += 1
        match = re.search(r"_Full$", str(path.parent))
        if match is not None and current_month_only:
            return pathlist[:index]
    return pathlist


def get_run_logger():
    return logging.getLogger("sra_etl")


def sra_parse(url: str, outfile_name: str):
    logger = get_run_logger()

    logger.info(f"Processing {url} to {outfile_name}")
    if UPath(outfile_name).exists():
        logger.info(f"{outfile_name} already exists. Skipping")
        return

    # while it is technically possible to stream from the URL
    # the safer way is to download the file first
    with tempfile.NamedTemporaryFile() as tmpfile:
        # download the file to a temporary file
        shutil.copyfileobj(UPath(url).open("rb"), tmpfile)
        # read the file and write to the output file
        tmpfile.seek(0)
        with gzip.open(tmpfile, "rb") as fh:
            with UPath(outfile_name).open("wb", compression="gzip") as outfile:
                # iterate over the objects and write them to the output file
                for obj in sra_object_generator(fh):
                    outfile.write(orjson.dumps(obj.data) + b"\n")


def get_pathlist():
    return mirror_dirlist_for_current_month()


def sra_get_urls():
    pathlist = get_pathlist()
    current_gcs_objects = []
    for parent in pathlist:
        p = parent.parent
        urls = list(p.glob("**/*xml.gz"))
        for url in urls:
            if url.name == "meta_analysis_set.xml.gz":
                continue
            path_part = url.parts[-2]
            xml_name = url.parts[-1]
            json_name = xml_name.replace(".xml.gz", ".ndjson.gz")
            outfile_name = f"{path_part}_{json_name}"
            sra_parse(
                url=str(url),
                outfile_name=f"gs://omicidx-json/prefect-testing/sra/{outfile_name}",
            )
            current_gcs_objects.append(
                UPath(f"gs://omicidx-json/prefect-testing/sra/{outfile_name}")
            )
    all_objects = UPath("gs://omicidx-json/prefect-testing/sra").glob("*set.ndjson.gz")
    for obj in all_objects:
        if obj not in current_gcs_objects:
            obj.unlink()


# register the flow
if __name__ == "__main__":
    sra_get_urls()
