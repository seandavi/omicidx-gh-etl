from . import db
from .. import logging
from prefect import task

logger = logging.get_logger("loader")


def get_sql(entity: str, plural_entity: str) -> str:
    sql = f"""
CREATE OR REPLACE TABLE src_sra__{plural_entity}
ENGINE=MergeTree()
ORDER BY tuple(accession)
SETTINGS 
    storage_policy='s3_main'
AS
SELECT 
    * EXCEPT accession,
    accession::String AS accession,
    _file,
    _path
FROM
s3('https://storage.googleapis.com/omicidx-json/sra/*{entity}_set.ndjson.gz', JSONEachRow);
"""
    return sql


@task
def load_entities_to_clickhouse():
    entities = {
        "study": "studies",
        "sample": "samples",
        "experiment": "experiments",
        "run": "runs",
    }
    for entity, plural_entity in entities.items():
        sql = get_sql(entity, plural_entity)
        client = db.get_client()
        res = client.command(sql)
        summary_info = res.summary  # type: ignore
        summary_info[entity] = plural_entity
        logger.info(f"Created table {entity}")
        logger.info(summary_info)


if __name__ == "__main__":
    load_entities_to_clickhouse()
