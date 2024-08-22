from . import db
from urllib.request import urlopen
import zipfile
import tarfile
import pathlib
import fsspec
import shutil
import httpx
from upath import UPath
from google.cloud import bigquery
from prefect import task, flow

from ..logging import get_logger

logger = get_logger(__name__)

PROJECT_ID = "gap-som-dbmi-sd-app-fq9"
DATASET_ID = "omicidx"
ICITE_COLLECTION_ID = 4586573


def get_run_logger():
    return get_logger("icite_etl")


logger = get_run_logger()


@task
def get_icite_collection_articles() -> list[dict[str, str]]:
    with httpx.Client(timeout=60) as client:
        response = client.get(
            f"https://api.figshare.com/v2/collections/{ICITE_COLLECTION_ID}/articles"
        )
        response.raise_for_status()
        logger.info("Getting latest ICITE articles from figshare")
        return response.json()


@task
def get_icite_article_files(article_id: str):
    with httpx.Client(timeout=60) as client:
        response = client.get(
            f"https://api.figshare.com/v2/articles/{article_id}/files"
        )
        response.raise_for_status()
        logger.info("Getting latest ICITE article files from figshare")
        return response.json()


@task
def expand_tarfile(tarfname: str, dest: str) -> list[str]:
    pathlib.Path(dest).mkdir(parents=True, exist_ok=True)

    with tarfile.open(tarfname) as tar:
        logger.info(f"Extracting {tarfname}")
        for fname in tar.getnames():
            if fname.endswith(".json"):
                logger.info(f"Extracting {fname}")
                tar.extract(fname, dest)
                logger.info(f"Uploading {fname} to GCS")
                up = UPath("gs://omicidx/icite")
                localfile = pathlib.Path(f"{dest}/{fname}")
                upfile = up / str(localfile.with_suffix(".jsonl.gz").name)
                with open(localfile, "rb") as lf:
                    with upfile.open("wb", compression="gzip") as uf:
                        shutil.copyfileobj(lf, uf)
                localfile.unlink(missing_ok=True)
                pathlib.Path(tarfname).unlink(missing_ok=True)


@task
def expand_zipfile(zipfname: str) -> str:
    logger.info(f"Extracting {zipfname}")
    with zipfile.ZipFile(zipfname) as zip:
        with zip.open("open_citation_collection.csv") as f:
            with UPath("gs://omicidx/icite/open_citation_collection.csv").open(
                "wb"
            ) as outfile:
                shutil.copyfileobj(f, outfile)
    return "open_citation_collection.csv"


@task
def download_icite_file(file_json: list[dict]) -> str:
    url = list(filter(lambda x: x["name"] == "icite_metadata.tar.gz", file_json))[0][
        "download_url"
    ]  # type: ignore
    with urlopen(url) as f:
        logger.info(f"Downloading {url}")
        shutil.copyfileobj(f, open("icite_metadata.tar.gz", "wb"))
    return "icite_metadata.tar.gz"


@task
def download_opencitation_file(file_json: list[dict]) -> str:
    url = list(
        filter(lambda x: x["name"] == "open_citation_collection.zip", file_json)
    )[0]["download_url"]
    print(url)
    with urlopen(url) as f:
        logger.info(f"Downloading {url}")
        shutil.copyfileobj(f, open("open_citation_collection.zip", "wb"))
    return "open_citation_collection.zip"


def get_gcs_fs():
    return fsspec.filesystem("gs")


def clean_out_gcs_dir(dir: str) -> None:
    fs = get_gcs_fs()
    logger.info(f"Cleaning out {dir}")
    try:
        fs.rm(dir, recursive=True)
    except FileNotFoundError:
        pass


def load_to_clickhouse():
    logger.info("Loading to clickhouse")
    client = db.get_client()
    sql = """
    CREATE OR REPLACE TABLE src_icite__metadata
    ENGINE=MergeTree()
    ORDER BY tuple(pmid)
    SETTINGS storage_policy='s3_main'
    AS
    SELECT 
        * EXCEPT (pmid, `references`, cited_by, cited_by_clin, authors, last_modified),
        pmid::UInt32 AS pmid,
        splitByWhitespace(COALESCE(`references`,'')) AS `references`,
        splitByWhitespace(COALESCE(cited_by,'')) AS cited_by,
        splitByWhitespace(COALESCE(cited_by_clin,'')) AS cited_by_clin,
        splitByString(', ', COALESCE(authors,'')) AS authors,
        parseDateTime64BestEffort(last_modified)::DateTime64 AS last_modified_at,
        now() AS _inserted_at
    FROM 
    s3('https://storage.googleapis.com/omicidx-json/icite/icite_metadata_*jsonl.gz', JSONEachRow);
    """
    logger.info(sql)
    res = client.command(sql)
    logger.info("created table src_icite__metadata")


@task
def load_to_bigquery():
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_TRUNCATE",
    )
    uri = "gs://omicidx-json/icite/icite*jsonl.gz"
    table_id = f"{PROJECT_ID}.{DATASET_ID}.src_icite"
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()
    logger.info(f"Loaded {load_job.output_rows} rows to {table_id}")


@flow
def icite_flow() -> tuple[list[str], str]:
    """Flow to ingest icite data from figshare

    The NIH ICITE data is stored in a figshare collection. This flow
    downloads the data from figshare, extracts the tarfile, and uploads
    the json files to GCS.

    Since there are updates to the data, the flow also cleans out the
    GCS directory before uploading the new data.

    The article is "updated" monthly, so the flow must first find
    the latest version of the data using the figshare API.

    """
    articles: list[dict] = get_icite_collection_articles()  # type: ignore
    files: list[dict] = get_icite_article_files(articles[0]["id"])  # type: ignore
    icite_tarfile = download_icite_file(files)
    icite_tarfile = "icite_metadata.tar.gz"
    opencitation_zipfile = download_opencitation_file(files)
    clean_out_gcs_dir("omicidx-json/icite")
    clean_out_gcs_dir("omicidx-json/opencitation")
    opencitation_file = expand_zipfile(opencitation_zipfile)  # type: ignore
    icite_files = expand_tarfile(icite_tarfile, "icite")  # type: ignore
    # load_to_clickhouse()
    return icite_files, opencitation_file


if __name__ == "__main__":
    # register_deployment()
    icite_flow()
