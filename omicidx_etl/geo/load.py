from google.cloud import bigquery
from .schema import get_schema


def load_to_bigquery(entity: str):
    """
    Load a GEO entity to BigQuery.

    Args:
        entity (str): The GEO entity to load.

    """
    client = bigquery.Client()
    schema = get_schema(entity)
    load_job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        schema=schema,
    )

    uri = f"gs://omicidx/geo/{entity}*ndjson.gz"
    dataset = "omicidx"
    table = f"src_geo__{entity}s"
    job = client.load_table_from_uri(
        uri, f"{dataset}.{table}", job_config=load_job_config
    )

    return job.result()  # Waits for the job to complete.


if __name__ == "__main__":
    for entity in ["gse", "gsm", "gpl"]:
        job_result = load_to_bigquery(entity)
        print(f"Loaded {entity} to BigQuery")
        print(job_result)
