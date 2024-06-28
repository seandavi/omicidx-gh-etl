from google.cloud import bigquery

schema = {}

schema["biosample"] = [
    bigquery.SchemaField("model", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("attributes", "STRING", mode="REPEATED"),
    bigquery.SchemaField("taxonomy_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("gsm", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ids", "STRING", mode="REPEATED"),
    bigquery.SchemaField(
        "id_recs",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("label", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("db", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("accession", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "attribute_recs",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("unit", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("display_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("harmonized_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attribute_name", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("sra_sample", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("access", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("publication_date", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("dbgap", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("last_update", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("submission_date", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("taxon_id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("is_reference", "STRING", mode="NULLABLE"),
]

schema["bioproject"] = [
    bigquery.SchemaField("data_types", "STRING", mode="REPEATED"),
    bigquery.SchemaField("accession", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "publications",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("db", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("pubdate", "TIMESTAMP", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField(
        "external_links",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("label", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
]


def get_schema(entity: str):
    return schema[entity]
