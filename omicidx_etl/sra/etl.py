import gzip
import re
import shutil
import tempfile
from datetime import datetime

import orjson
from omicidx.sra.parser import sra_object_generator
from prefect import flow, task
import prefect
from upath import UPath

from ..config import settings
from ..db import duckdb_connection
from ..logging import get_logger
from .state import CompactSRAState, sync_load_sra_state, sync_save_sra_state
from .utils import bigquery_load

logger = get_logger(__name__)

OUTPUT_PATH = UPath(settings.PUBLISH_DIRECTORY) / "sra"
OUTPUT_DIR = str(OUTPUT_PATH)


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
    pathlist = sorted(list(u.glob("**/")), reverse=True)
    index = 0
    for path in pathlist:
        index += 1
        match = re.search(r"_Full$", str(path.parent))
        if match is not None and current_month_only:
            return pathlist[:index]
    return pathlist


@task(task_run_name="sra-parse-{url}")
def sra_parse(url: str, outfile_name: str, state: CompactSRAState):
    logger.info(f"Processing {url} to {outfile_name}")
    
    # Check if already processed
    if state.is_url_processed(url):
        logger.info(f"{url} already processed. Skipping")
        return state

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
        record_count = 0
        
        # Write to temporary output file first
        with tempfile.NamedTemporaryFile(dir="./", delete=False) as tmp_output:
            with gzip.open(tmpfile, "rb") as fh:
                with gzip.open(tmp_output, "wb") as outfile:
                    # iterate over the objects and write them to the output file
                    for obj in sra_object_generator(fh):
                        outfile.write(orjson.dumps(obj.data) + b"\n")
                        record_count += 1
            tmp_output.flush()
            
            # Copy the temporary file to final destination
            logger.info(f"Copying output to {outfile_name}")
            try:
                with open(tmp_output.name, "rb") as temp_src:
                    with UPath(outfile_name).open("wb") as final_dest:
                        shutil.copyfileobj(temp_src, final_dest)
                logger.info(f"Successfully copied output to {outfile_name}")
            except Exception as e:
                logger.error(f"Failed to copy output to {outfile_name}: {e}")
                raise
            finally:
                # Clean up temporary output file
                try:
                    UPath(tmp_output.name).unlink()
                except Exception as e:
                    logger.warning(f"Failed to remove temporary file {tmp_output.name}: {e}")
        
        # Update state
        state.add_processed_url(url)
        
        # Mark full dump time if this is a full dump
        if "_Full" in url:
            state.last_full_dump = datetime.now()
        
        logger.info(f"Processed {record_count} records from {url}")
        return state


def get_pathlist():
    return mirror_dirlist_for_current_month()


@task(task_run_name="load-{entity}-to-bigquery")
def task_load_entities_to_bigquery(entity: str, plural_entity: str):
    bigquery_load(entity, plural_entity)
    
    
def sra_json_to_parquet(entity):
    entities = {
        "study": "studies",
        "sample": "samples",
        "experiment": "experiments",
        "run": "runs",
    }
    with duckdb_connection() as con:
        sql = f"""
        copy (select * from read_ndjson_auto(
            'r2://biodatalake/sra/NCBI*{entity}_set.ndjson.gz',union_by_name=True)
        ) to 'r2://biodatalake/sra/{entities[entity]}.parquet' 
        (format parquet, compression zstd)"""
        con.execute(sql)
        logger.info(f"created file biodatalake/sra/{entities[entity]}.parquet")
        
@task(task_run_name="sra-json-to-parquet-{entity}")
def sra_json_to_parquet_task(entity: str):
    sra_json_to_parquet(entity)
    
def sra_accessions_tsv_to_parquet():
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
        logger.info(f"created file biodatalake/sra/accessions.parquet")

@task(task_run_name="sra-accessions-to-parquet")
def sra_accessions_tsv_to_parquet_task():
    sra_accessions_tsv_to_parquet()


def json_outfile_name_from_url(url: str) -> str:
    """Generate an output file name based on the URL."""
    url_path = UPath(url)
    path_part = url_path.parts[-2]
    xml_name = url_path.parts[-1]
    json_name = xml_name.replace(".xml.gz", ".ndjson.gz")
    return f"{path_part}_{json_name}"


def get_task_runner() -> prefect.task_runners.TaskRunner:
    """Get the task runner for Prefect."""
    from prefect.task_runners import ConcurrentTaskRunner
    return ConcurrentTaskRunner(max_workers=4)

@flow(task_runner=get_task_runner())
def sra_get_urls():
    # Load current state
    state = sync_load_sra_state()
    current_month = datetime.now().strftime("%Y-%m")
    
    # Reset state if we're in a new month
    # TODO: We should use the presence of a new
    # full dump to reset the state, not just the month
    if state.current_month != current_month:
        logger.info(f"New month detected ({current_month}). Resetting state.")
        state = CompactSRAState(current_month=current_month)
    
    pathlist = get_pathlist()
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
    
    # Get unprocessed partitions
    unprocessed_urls = state.get_unprocessed_urls(all_urls)
    logger.info(f"Found {len(unprocessed_urls)} unprocessed partitions out of {len(all_urls)} total")
    
    # Process unprocessed partitions

    for url in unprocessed_urls:
        outfile_name = json_outfile_name_from_url(url)
        
        state = sra_parse(
            url=url,
            outfile_name=f"{OUTPUT_DIR}/{outfile_name}",
            state=state
        )
        
        
        # Save state periodically
        sync_save_sra_state(state)
    

    
    # Process entities that haven't been completed
    if not state.is_entity_complete("accessions"):
        sra_accessions_tsv_to_parquet_task()
        state.mark_entity_complete("accessions")
        sync_save_sra_state(state)
    
    # Uncomment when ready to process entities
    # for entity in ["study", "sample", "experiment", "run"]:
    #     if not state.is_entity_complete(entity):
    #         sra_json_to_parquet_task(entity)
    #         state.mark_entity_complete(entity)
    #         sync_save_sra_state(state)
    
    # Final state save
    sync_save_sra_state(state)
    logger.info("SRA ETL processing complete")


# register the flow
if __name__ == "__main__":
    sra_get_urls()
    # Final state save
    logger.info("SRA ETL processing complete")
