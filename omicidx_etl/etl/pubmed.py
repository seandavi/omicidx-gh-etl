import logging
from upath import UPath
import re
import datetime
import pubmed_parser as pp
import orjson
from urllib.request import urlretrieve
import tempfile
import shutil

JOB_NAME = "projects/omicidx-338300/locations/us-central1/jobs/pubmed-builder"
PUBMED_BASE = UPath("https://ftp.ncbi.nlm.nih.gov/pubmed")
OUTPUT_UPATH = UPath("gs://omicidx-json/pubmed")

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


class PubmedManager:
    def __init__(
        self,
        base_url: UPath,
        output_url: UPath,
    ):
        """Create a PubmedManager object.

        Args:
            base_url (UPath): The base url of the pubmed files.
            output_url (UPath): The output url of the pubmed files.
        """
        self.base_url = base_url
        self.output_url = output_url
        self.load_existing()
        self.load_available()

    def _url_to_pubmed_id(self, url: UPath) -> str:
        """Get the pubmed id from the url."""
        return re.sub(r"\..*", "", url.name)

    def load_available(self):
        """Load the available urls from the base directory."""
        available_urls = list(self.base_url.glob("**/pubmed*.xml.gz"))
        id_to_available_url_map = {
            self._url_to_pubmed_id(url): url for url in available_urls
        }
        self.available_urls = id_to_available_url_map

    def load_existing(self):
        """Load the existing urls from the output directory."""
        existing_urls = list(self.output_url.glob("**/*.jsonl.gz"))
        id_to_existing_url_map = {
            self._url_to_pubmed_id(url): url for url in existing_urls
        }
        self.existing_urls = id_to_existing_url_map

    def needed_ids(self, replace=False):
        """Return the ids that are needed to be processed."""
        in_ids = set(self.available_urls.keys())
        out_ids = set(self.existing_urls.keys())

        if replace:
            return in_ids

        return in_ids - out_ids

    def needed_urls(self, replace=False) -> list[UPath]:
        """Return the urls that are needed to be processed."""
        return [self.available_urls[id] for id in self.needed_ids(replace=replace)]


def json_file_for_url(url: UPath, outpath: UPath) -> UPath:
    fname_out = url.name.replace(".xml.gz", ".ndjson.gz")
    return outpath / fname_out


def pubmed_url_to_json_file(url: UPath, outpath: UPath) -> None:
    """Pubmed files as json asset

    This asset covers the entire pubmed corpus. It is partitioned by
    pubmed file. Each partition is a line iterator that yields json
    objects for each article in the pubmed file after conversion from
    xml to json. The json objects are serialized to bytes using orjson.
    """
    with tempfile.NamedTemporaryFile(suffix=".xml.gz") as f:
        localfname = f.name
        urlretrieve(str(url), filename=localfname)
        generator = pp.parse_medline_xml(
            localfname,
            year_info_only=False,
            nlm_category=True,
            author_list=True,
            reference_list=True,
        )
        with tempfile.NamedTemporaryFile() as outfile:
            logger.info(f"Writing {url} to {outfile.name}")
            for obj in generator:
                obj["_inserted_at"] = datetime.datetime.now()
                obj["_read_from"] = str(url)
                outfile.write(orjson.dumps(obj) + b"\n")
            outfile.seek(0)
            finaloutfile = json_file_for_url(url, outpath)
            shutil.copyfileobj(
                outfile,
                finaloutfile.open(
                    "wb", compression="gzip", block_size=1024 * 1024 * 10
                ),
            )


def get_pubmeds(replace: bool = False):
    pubmed_manager = PubmedManager(PUBMED_BASE, OUTPUT_UPATH)
    needed_urls = pubmed_manager.needed_urls(replace=replace)
    for url in needed_urls:
        logger.info("Processing url: " + str(url))
        pubmed_url_to_json_file(url, OUTPUT_UPATH)


if __name__ == "__main__":
    get_pubmeds()
