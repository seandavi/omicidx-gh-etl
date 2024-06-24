from . import db
from google.cloud import bigquery


sql = """
CREATE OR REPLACE TABLE
src_pubmed__metadata
ENGINE=MergeTree()
ORDER BY tuple(pmid)
SETTINGS 
  storage_policy='s3_main'
AS
SELECT 
  * EXCEPT pmid,
  pmid::UInt32 AS pmid
FROM 
s3('https://storage.googleapis.com/omicidx-json/pubmed/*jsonl.gz', JSONEachRow)
SETTINGS 
max_table_size_to_drop='100G';
"""


def load_pubmed_to_clickhouse():
    client = db.get_client()
    client.command(sql)


def get_bigquery_schema():
    schema = [
        bigquery.SchemaField("_inserted_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("_read_from", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("abstract", "STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            "authors",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("affiliation", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("forename", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("identifier", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("initials", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("lastname", "STRING", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField("chemical_list", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("delete", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("doi", "STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            "grant_ids",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("agency", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("grant_acronym", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("grant_id", "STRING", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField("issn_linking", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("issue", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("journal", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("keywords", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("languages", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("medline_ta", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("mesh_terms", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("nlm_unique_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("other_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pages", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pmc", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pmid", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pubdate", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("publication_types", "STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            "references",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("citation", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("pmid", "STRING", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("vernacular_title", "STRING", mode="NULLABLE"),
    ]
    return schema


def load_to_bigquery():
    client = bigquery.Client()
    schema = get_bigquery_schema()
    load_job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        schema=schema,
    )

    uri = "gs://omicidx-json/pubmed/*jsonl.gz"
    dataset = "biodatalake"
    table = "src_pubmed__metadata"
    job = client.load_table_from_uri(
        uri, f"{dataset}.{table}", job_config=load_job_config
    )

    job.result()  # Waits for the job to complete.


if __name__ == "__main__":
    load_to_bigquery()
    exit()
    load_pubmed_to_clickhouse()
    print("Done")
