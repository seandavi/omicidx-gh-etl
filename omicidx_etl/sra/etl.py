"""
SRA ETL Pipeline with Prefect (DEPRECATED)

⚠️  DEPRECATION NOTICE ⚠️
This module is deprecated and will be removed in a future version.
Please use the new simplified SRA extraction instead:

  from omicidx_etl.sra.extract import extract_sra
  # or via CLI: omicidx-etl sra extract <output_dir>

The new implementation:
- Removes Prefect dependencies and complexity
- Provides both Parquet and NDJSON output formats  
- Uses efficient parallel processing optimized for local servers
- Supports direct R2 upload with UPath
- Offers a clean Click-based CLI interface

For migration help, see: omicidx_etl/sra/README.md
"""

import gzip
import re
import shutil
import tempfile
import pathlib
from prefect.artifacts import LinkArtifact, MarkdownArtifact

import orjson
from omicidx.sra.parser import sra_object_generator
from prefect import flow, task
from prefect.futures import wait
import prefect
from prefect.cache_policies import INPUTS, TASK_SOURCE
from upath import UPath
from prefect_aws.s3 import S3Bucket
from pydantic import BaseModel, Field
from datetime import timedelta

from ..config import settings
from ..db import duckdb_connection
from ..logging import get_logger
from .utils import bigquery_load
from dotenv import load_dotenv

logger = get_logger(__name__)

OUTPUT_PATH = UPath(settings.PUBLISH_DIRECTORY) / "sra"
OUTPUT_DIR = str(OUTPUT_PATH)
load_dotenv()

result_storage = S3Bucket.load('osn-bucket')

class SRAProcessingResult(BaseModel):
    url: str
    outfile_name: str
    record_count: int
    output_path: str = Field(default=OUTPUT_DIR, description="Base output directory for SRA processing")


def directories_from_last_full() -> list[UPath]:
    """return a list directories for the most recent full and all associated incrementals.

    The NCBI SRA mirror directory is organized by date. This function finds the
    most recent full mirror directory for and all directories of
    incremental updates for the current month.

    Returns:
        list[UPath]: a list of UPath objects
    """
    u = UPath("https://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring")
    pathlist = sorted(list(u.glob("**/")), reverse=True)
    index = 0
    for path in pathlist:
        index += 1
        match = re.search(r"_Full$", str(path.parent))
        if match is not None:
            return pathlist[:index]
    return pathlist

def sra_download(url: str, tmpfile: pathlib.Path):
    """Download SRA file and return path to temporary file."""
    
    shutil.copyfileobj(UPath(url).open("rb"), tmpfile.open("wb"))

def sra_process(url: str, tmpfile: pathlib.Path, tmp_output: pathlib.Path) -> int:
    
    record_count = 0
    
    with gzip.open(tmpfile, "rb") as fh:
        with gzip.open(tmp_output, "wb") as outfile:
            # iterate over the objects and write them to the output file
            for obj in sra_object_generator(fh):
                outfile.write(orjson.dumps(obj.data) + b"\n")
                record_count += 1
    
    logger.info(f"Processed {record_count} records from {url}")
    return record_count
    
        
def sra_upload(url: str, local_jsonlines_path: pathlib.Path, record_count: int, outfile_name: str):
    """Upload processed file to final destination and return result."""

    with open(local_jsonlines_path, "rb") as temp_src:
        with UPath(outfile_name).open("wb") as final_dest:
            shutil.copyfileobj(temp_src, final_dest)

@task(
    task_run_name="sra-parse-pipeline-{url}",
    retries=3,
    retry_delay_seconds=10,
    cache_policy=TASK_SOURCE + INPUTS,
    persist_result=True,
    result_storage=result_storage  # type: ignore
)
def sra_parse_pipeline(url: str, outfile_name: str) -> SRAProcessingResult:
    """Pipeline function that orchestrates download, process, and upload."""
    # Download
    with (tempfile.NamedTemporaryFile()) as local_xml_file:
        logger.info(f"Downloading {url}")
        sra_download(url, pathlib.Path(local_xml_file.name))
        
        # Process
        logger.info(f"Processing {url}") 
        with tempfile.NamedTemporaryFile() as local_jsonlines_file:
            record_count = sra_process(url, pathlib.Path(local_xml_file.name), pathlib.Path(local_jsonlines_file.name))
        
            # Upload (only this returns a persisted result)
            logger.info(f"Uploading {url} to {outfile_name}")
            sra_upload(url, pathlib.Path(local_jsonlines_file.name), record_count, outfile_name)
            
    return SRAProcessingResult(
        url=url,
        outfile_name=outfile_name,
        record_count=record_count,
        output_path=OUTPUT_DIR
    )



def sra_json_to_parquet(entity):
    entities = {
        "study": "studies",
        "sample": "samples",
        "experiment": "experiments",
        "run": "runs",
    }
    with duckdb_connection() as con:
        logger.info(f"Loading {entity} to Parquet")
        sql = f"""
        copy (select * from read_ndjson_auto(
            'r2://biodatalake/sra/NCBI*{entity}_set.ndjson.gz',union_by_name=True)
        ) to 'r2://biodatalake/sra/{entities[entity]}.parquet' 
        (format parquet, compression zstd)"""
        con.execute(sql)
        logger.info(f"created file biodatalake/sra/{entities[entity]}.parquet")
    
        
@task(
    task_run_name="sra-json-to-parquet-{entity}",
    cache_policy = TASK_SOURCE + INPUTS,
    persist_result=True,
    result_storage=result_storage,  # type: ignore
    cache_expiration=timedelta(days=1) 
)
def sra_json_to_parquet_task(entity: str):
    sra_json_to_parquet(entity)
    return f"Processed {entity} to Parquet successfully"

@task(task_run_name="sra-accessions-to-parquet")
def sra_accessions_tsv_to_parquet():
    logger.info("Converting SRA accessions TSV to Parquet")
    with duckdb_connection() as con:
        sql = """
        copy 
            (
                select 
                    * 
                from 
                    read_csv_auto(
                        'https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab', 
                        delim='\t',
                        nullstr='-'
                    )) 
                to 
                    'r2://biodatalake/sra/sra_accessions.parquet' 
                    (
                        format parquet, 
                        compression zstd
                    )
        """
        con.execute(sql)
        logger.info("created file biodatalake/sra/accessions.parquet")


def json_outfile_name_from_url(url: str) -> str:
    """Utility functionn to generate an output file name based on the URL."""
    url_path = UPath(url)
    path_part = url_path.parts[-2]
    xml_name = url_path.parts[-1]
    json_name = xml_name.replace(".xml.gz", ".ndjson.gz")
    return f"{path_part}_{json_name}"


def get_task_runner() -> prefect.task_runners.TaskRunner:
    """Get the task runner for Prefect."""
    from prefect.task_runners import ConcurrentTaskRunner
    return ConcurrentTaskRunner(max_workers=8)

@flow(task_runner=get_task_runner())
def sra_get_urls():
   
    logger = get_logger() 
    logger.info("Starting SRA ETL process")
     
    pathlist = directories_from_last_full()
    current_gcs_objects = []
    all_urls = []

    # Collect all URLs first
    for parent in pathlist:
        p = parent.parent
        urls = list(p.glob("**/*xml.gz"))
        logger.info(f"Found URLs in {p}: {len(urls)}")
        for url in urls:
            if url.name == "meta_analysis_set.xml.gz":
                continue
            all_urls.append(str(url))
            current_gcs_objects.append(UPath(OUTPUT_DIR) / json_outfile_name_from_url(str(url)))
            
    # Clean up old objects not in current processing
    logger.info('Cleaning up old objects not in current processing')
    all_objects = UPath(OUTPUT_DIR).glob("*set.ndjson.gz")
    
    for obj in all_objects:
        if obj not in current_gcs_objects:
            try:
                obj.unlink()
                logger.info(f"Deleted {obj}")
            except Exception as e:
                logger.error(f"Failed to delete {obj}: {e}")
   
    futures = [] 
    futures.append(sra_accessions_tsv_to_parquet.submit())  # type: ignore
    for url in all_urls:
        outfile_name = json_outfile_name_from_url(url)

        futures.append(sra_parse_pipeline.submit(  # type: ignore
            url=url,
            outfile_name=f"{OUTPUT_DIR}/{outfile_name}",
        ))
        
    wait(futures)

    # Transform JSON to Parquet
    logger.info("Transforming JSON to Parquet")

    table_futures = sra_json_to_parquet_task.map(["study", "sample", "experiment", "run"])  # type: ignore
    wait(table_futures)
    
# register the flow
if __name__ == "__main__":
    sra_get_urls()
