from .. import logging
from prefect import task
from google.cloud import bigquery
from .schema import get_schema

logger = logging.get_logger("loader")


def bigquery_load(entity: str, plural_entity: str):
    client = bigquery.Client()

    schema = get_schema(entity)

    load_job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        schema=schema,
    )

    uri = f"gs://omicidx/sra/*{entity}_set.ndjson.gz"
    dataset = "omicidx"
    table = f"src_sra__{plural_entity}"
    job = client.load_table_from_uri(
        uri, f"{dataset}.{table}", job_config=load_job_config
    )

    return job.result()  # Waits for the job to complete.


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


@task(task_run_name="load-{entity}-to-clickhouse")
def load_entities_to_clickhouse(entity: str, plural_entity: str):
    sql = get_sql(entity, plural_entity)
    client = db.get_client()
    res = client.command(sql)
    summary_info = res.summary  # type: ignore
    summary_info[entity] = plural_entity
    logger.info(f"Created table {entity}")
    logger.info(summary_info)


if __name__ == "__main__":
    entities = {
        "study": "studies",
        "sample": "samples",
        "experiment": "experiments",
        "run": "runs",
    }
    for entity, plural_entity in entities.items():
        load_entities_to_clickhouse(entity, plural_entity)
