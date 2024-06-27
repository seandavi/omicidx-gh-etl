from google.cloud import bigquery

schema = {}

schema["run"] = [
    bigquery.SchemaField("accession", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("alias", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "attributes",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("tag", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("broker_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("center_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("experiment_accession", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "identifiers",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("namespace", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("uuid", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("run_center", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("run_date", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
]

schema["study"] = [
    bigquery.SchemaField("pubmed_ids", "STRING", mode="REPEATED"),
    bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("update_date", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("meta_update_date", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("publish_date", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("received_date", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("visibility", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("insdc", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("accession", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("alias", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("center_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("broker_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "attributes",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("tag", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField(
        "identifiers",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("namespace", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField(
        "xrefs",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("db", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("study_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("study_accession", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("abstract", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("BioProject", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("GEO", "STRING", mode="NULLABLE"),
]

schema["experiment"] = [
    bigquery.SchemaField("accession", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("alias", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "attributes",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("tag", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("broker_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("center_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("library_layout_orientation", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("GEO", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("design", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("experiment_accession", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "identifiers",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("namespace", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("uuid", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("instrument_model", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("library_construction_protocol", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("library_layout", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("library_layout_length", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("library_layout_sdev", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("library_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("library_selection", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("library_source", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("library_strategy", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("nreads", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("platform", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "reads",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("base_coord", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("read_class", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("read_index", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("read_type", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("sample_accession", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("spot_length", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("study_accession", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "xrefs",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("db", "STRING", mode="NULLABLE"),
        ],
    ),
]

schema["sample"] = [
    bigquery.SchemaField("accession", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("broker_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("alias", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "attributes",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("tag", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("BioSample", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("center_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("GEO", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "identifiers",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("uuid", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("namespace", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("organism", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("sample_accession", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("taxon_id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "xrefs",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("db", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
        ],
    ),
]


def get_schema(entity: str) -> list[bigquery.SchemaField]:
    """Get the schema for a given entity.

    Args:
        entity (str): The entity to get the schema for. One of "run", "study", "experiment", "sample".
    """
    return schema[entity]
