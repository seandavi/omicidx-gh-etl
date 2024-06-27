from omicidx.sra.parser import sra_object_generator
from upath import UPath
from prefect import task, flow
from .utils import bigquery_load


import orjson
import gzip
import tempfile
import shutil

import re

from ..logging import get_logger


logger = get_logger(__name__)


def mirror_dirlist_for_current_month(current_month_only: bool = True) -> list[UPath]:
    """return a list of UPath objects for the current month's mirror directories

    The NCBI SRA mirror directory is organized by date. This function finds the
    most recent full mirror directory for the current month and all directories of
    incremental updates for the current month.

    Returns:
        list[UPath]: a list of UPath objects

    >>> mirror_dirlist_for_current_month()
    """
    u = UPath("https://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring")
    pathlist = sorted(list(u.glob("*")), reverse=True)
    index = 0
    for path in pathlist:
        index += 1
        match = re.search(r"_Full$", str(path.parent))
        if match is not None and current_month_only:
            return pathlist[:index]
    return pathlist


@task
def sra_parse(url: str, outfile_name: str):
    logger.info(f"Processing {url} to {outfile_name}")
    if UPath(outfile_name).exists():
        logger.info(f"{outfile_name} already exists. Skipping")
        return

    # while it is technically possible to stream from the URL
    # the safer way is to download the file first
    # I use the './' directory for github actions where the
    # tempfile system is small compared to the working directory
    # Avoids the "OSError: No space left on device"
    with tempfile.NamedTemporaryFile(dir="./") as tmpfile:
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


@task(task_run_name="load-{entity}-to-bigquery")
def task_load_entities_to_bigquery(entity: str, plural_entity: str):
    bigquery_load(entity, plural_entity)


@flow
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
                outfile_name=f"gs://omicidx-json/sra/{outfile_name}",
            )
            current_gcs_objects.append(UPath(f"gs://omicidx-json/sra/{outfile_name}"))
    all_objects = UPath("gs://omicidx-json/sra").glob("*set.ndjson.gz")
    for obj in all_objects:
        if obj not in current_gcs_objects:
            logger.info(f"Deleting old {obj}")
            obj.unlink()
    entities = {
        "study": "studies",
        "sample": "samples",
        "experiment": "experiments",
        "run": "runs",
    }
    for entity, plural_entity in entities.items():
        task_load_entities_to_bigquery(entity, plural_entity)


# register the flow
if __name__ == "__main__":
    sra_get_urls()
