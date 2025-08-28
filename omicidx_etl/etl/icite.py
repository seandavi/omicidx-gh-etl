from urllib.request import urlopen
import zipfile
import tarfile
import shutil
import click
import httpx
from upath import UPath
from dotenv import load_dotenv
import os
import gzip
from loguru import logger
import tempfile

load_dotenv(".env")
print(os.getenv("R2_ACCESS_KEY_ID"))
print(os.getenv("R2_SECRET_ACCESS_KEY"))

PROJECT_ID = "gap-som-dbmi-sd-app-fq9"
DATASET_ID = "omicidx"
ICITE_COLLECTION_ID = 4586573




def get_icite_collection_articles() -> list[dict[str, str]]:
    with httpx.Client(timeout=60) as client:
        response = client.get(
            f"https://api.figshare.com/v2/collections/{ICITE_COLLECTION_ID}/articles"
        )
        response.raise_for_status()
        logger.info("Getting latest ICITE articles from figshare")
        return response.json()



def get_icite_article_files(article_id: str):
    with httpx.Client(timeout=60) as client:
        response = client.get(
            f"https://api.figshare.com/v2/articles/{article_id}/files"
        )
        response.raise_for_status()
        logger.info("Getting latest ICITE article files from figshare")
        return response.json()



def expand_tarfile(tarfname: UPath, extraction_directory: UPath) -> list[UPath]:
    return_files = []
    with tarfile.open(tarfname) as tar:
        logger.info(f"Extracting {tarfname}")
        for tar_element in tar.getnames():
            fname = tar_element.split("/")[-1]
            if fname.endswith(".json"):
                logger.info(f"Extracting {fname}")
                tar.extract(tar_element, extraction_directory.name)
                localfile = extraction_directory / fname
                localgzip = localfile.with_suffix(".jsonl.gz")
                
                with localfile.open('rb') as f:
                    with gzip.open(localgzip,'wb') as gz:
                        shutil.copyfileobj(f, gz)
                localfile.unlink(missing_ok=True)
                return_files.append(localgzip)

    return return_files

def expand_zipfile(zipfile_path: UPath, outpath: UPath) -> UPath:
    logger.info(f"Extracting {zipfile_path}")
    outfile_path = outpath / "open_citation_collection.csv.gz"
    with zipfile.ZipFile(zipfile_path) as zip:
        with zip.open("open_citation_collection.csv") as f:
            with gzip.open(outfile_path, "wb") as gz:
                shutil.copyfileobj(f, gz)
    
    return outfile_path



def download_icite_file(file_json: list[dict], workpath: UPath) -> UPath:
    url = list(filter(lambda x: x["name"] == "icite_metadata.tar.gz", file_json))[0][
        "download_url"
    ]  # type: ignore
    icite_tarfile = workpath / "icite_metadata.tar.gz"
    with urlopen(url) as f:
        logger.info(f"Downloading {url}")
        shutil.copyfileobj(f, open(icite_tarfile, "wb"))
    return icite_tarfile

def download_opencitation_file(file_json: list[dict], workpath: UPath) -> UPath:
    url = list(
        filter(lambda x: x["name"] == "open_citation_collection.zip", file_json)
    )[0]["download_url"]
    opencitation_zipfile = workpath / "open_citation_collection.zip"
    with urlopen(url) as f:
        logger.info(f"Downloading {url}")
        shutil.copyfileobj(f, open(opencitation_zipfile, "wb"))
    return opencitation_zipfile 

def icite_flow(output_directory: UPath) -> list[UPath]:
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
    # open a temporary directory for all this work:
    with tempfile.TemporaryDirectory() as workdir:
        workpath = UPath(workdir)
        workpath.mkdir(parents=True, exist_ok=True)
        icite_tarfile = download_icite_file(files, workpath)
        opencitation_zipfile = download_opencitation_file(files, workpath)
        
        output_path = UPath("/tmp/omicidx/icite")
        output_path.mkdir(parents=True, exist_ok=True)
        
        extracted_files = expand_tarfile(icite_tarfile, output_path)
        opencitation_file = expand_zipfile(opencitation_zipfile, output_path)

    return extracted_files + [opencitation_file]


@click.group()
def icite():
    """ICITE extraction commands."""
    pass

@icite.command()
@click.argument('output_dir', type=click.Path(path_type=UPath))
def extract(output_dir: UPath):
    icite_flow(output_dir)
    
if __name__ == "__main__":
    extract()