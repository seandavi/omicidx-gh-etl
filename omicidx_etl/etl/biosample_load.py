from . import db
from .. import logging

logger = logging.get_logger("biosample_loader")


def get_sql(entity: str, plural_entity: str) -> str:
    sql = f"""
    CREATE OR REPLACE TABLE
    src_ncbi__{plural_entity}
    ENGINE=MergeTree()
    ORDER BY tuple(accession)
    SETTINGS 
      storage_policy='s3_main'
    AS
    SELECT 
      * EXCEPT accession,
      accession::String AS accession
    FROM 
    s3('https://storage.googleapis.com/omicidx-json/biosample/{entity}.ndjson.gz');
    """

    return sql


def load(entity: str, plural_entity: str) -> None:
    client = db.get_client()
    sql = get_sql(entity, plural_entity)
    res = client.command(sql)
    logger.info(f"Created table {entity}")
    summary_info = res.summary  # type: ignore
    summary_info["entity"] = plural_entity
    logger.info(f"Summary: {summary_info}")


if __name__ == "__main__":
    load("biosample", "biosamples")
    load("bioproject", "bioprojects")
