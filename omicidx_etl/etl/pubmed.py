import re
import datetime
import pubmed_parser as pp
from urllib.request import urlretrieve
import tempfile
from prefect import task, flow
from .pubmed_load import load_to_bigquery
import shutil
import gzip
from upath import UPath
import pyarrow as pa
import pyarrow.parquet as pq
import dbos

from ..logging import get_logger

# Module-level constants
PUBMED_BASE = UPath("https://ftp.ncbi.nlm.nih.gov/pubmed")
OUTPUT_UPATH = UPath("s3://biodatalake/pubmed")
OUTPUT_EXTENSION = ".parquet"

logger = get_logger(__name__)


def _url_to_pubmed_id(url: UPath) -> str:
    """Get the pubmed id from the url.
    
    For example, for a URL like `https://ftp.ncbi.nlm.nih.gov/pubmed/pubmed25n0023.xml.gz`
    it will return `pubmed25n0023`.
    """
    return re.sub(r"\..*", "", url.name)


def load_available_urls():
    """Load the available urls from the base directory.
    
    Note that this function covers both the base and update URLs."""
    available_urls = list(PUBMED_BASE.glob("**/pubmed*.xml.gz"))
    id_to_available_url_map = {
        _url_to_pubmed_id(url): url for url in available_urls
    }
    return id_to_available_url_map


def load_existing_urls():
    """Load the existing urls from the output directory."""
    existing_urls = list(OUTPUT_UPATH.glob(f"**/*{OUTPUT_EXTENSION}"))
    id_to_existing_url_map = {
        _url_to_pubmed_id(url): url for url in existing_urls
    }
    return id_to_existing_url_map


def get_needed_ids(replace=False):
    """Return the ids that are needed to be processed."""
    available_urls = load_available_urls()
    existing_urls = load_existing_urls()
    
    in_ids = set(available_urls.keys())
    out_ids = set(existing_urls.keys())

    if replace:
        return in_ids

    return in_ids - out_ids


def get_needed_urls(replace=False) -> list[UPath]:
    """Return the urls that are needed to be processed."""
    available_urls = load_available_urls()
    needed_ids = get_needed_ids(replace=replace)
    return [available_urls[id] for id in needed_ids]


def json_file_for_url(url: UPath) -> UPath:
    """Get the json output file path for a given URL."""
    fname_out = url.name.replace(".xml.gz", OUTPUT_EXTENSION)
    return OUTPUT_UPATH / fname_out


def parquet_file_for_url(url: UPath) -> UPath:
    """Get the parquet output file path for a given URL."""
    fname_out = url.name.replace(".xml.gz", ".parquet")
    return OUTPUT_UPATH / fname_out


def pubmed_url_to_parquet_file(url: UPath) -> None:
    """Pubmed files as parquet asset

    This asset covers the entire pubmed corpus. It is partitioned by
    pubmed file. Each partition is a line iterator that yields json
    objects for each article in the pubmed file after conversion from
    xml to json. The json objects are serialized to bytes using orjson.
    """
    with (
        tempfile.NamedTemporaryFile(suffix=".xml.gz") as f,
        tempfile.NamedTemporaryFile(suffix=".parquet") as local_parquet_file
    ):
        localfname = f.name
        urlretrieve(str(url), filename=localfname)
        generator = pp.parse_medline_xml(
            localfname,
            year_info_only=False,
            nlm_category=True,
            author_list=True,
            reference_list=True,
            parse_downto_mesh_subterms=True,
        )
        #with self.json_file_for_url(url).open("wb", compression="gzip") as outfile:
        
        objects = []
        with gzip.open('pubmed.jsonl.gz', 'wb') as outfile:
            for obj in generator:
                obj["_inserted_at"] = datetime.datetime.now()
                obj["_read_from"] = str(url)
                objects.append(obj)
        pubmed_table = pa.Table.from_pylist(objects)
        # write the table to parquet locally first
        # then upload it to the final destination
        pq.write_table(pubmed_table, local_parquet_file.name, compression='zstd')
        with parquet_file_for_url(url).open("wb") as outfile:
            logger.info(f"Writing {url} to {str(outfile)}")
            with open(local_parquet_file.name, 'rb') as infile:
                shutil.copyfileobj(infile, outfile)
            logger.info(f"Finished writing {url} to {str(outfile)}")



def etl_pubmeds(replace: bool = False):
    needed_urls = get_needed_urls(replace=replace)
    logger.info(f"Processing {len(needed_urls)} urls")
    for index, url in enumerate(needed_urls):
        logger.info("Processing url: " + str(url))
        logger.info(f"Processing {index + 1} of {len(needed_urls)}")
        pubmed_url_to_parquet_file(url)  # type: ignore

