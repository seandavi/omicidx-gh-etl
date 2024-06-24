from google.cloud import bigquery

schema = {}
schema["gse"] = [
    bigquery.SchemaField("manufacture_protocol", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("relation", "STRING", mode="REPEATED"),
    bigquery.SchemaField("data_row_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("manufacturer", "STRING", mode="REPEATED"),
    bigquery.SchemaField("distribution", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("series_id", "STRING", mode="REPEATED"),
    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("sample_id", "STRING", mode="REPEATED"),
    bigquery.SchemaField("summary", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "contact",
        "RECORD",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField("phone", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("institute", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("web_link", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("department", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                "name",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("last", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("middle", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("first", "STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("zip_postal_code", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("technology", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("accession", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("contributor", "JSON", mode="NULLABLE"),
    bigquery.SchemaField("last_update_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("submission_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("organism", "STRING", mode="NULLABLE"),
]


schema["gsm"] = [
    bigquery.SchemaField("submission_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField(
        "channels",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField(
                "characteristics",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("tag", "STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField("treatment_protocol", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("extract_protocol", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("label_protocol", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("source_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("organism", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("molecule", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("taxid", "INTEGER", mode="REPEATED"),
            bigquery.SchemaField("growth_protocol", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("label", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("overall_design", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("library_source", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("data_row_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("data_processing", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("channel_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("platform_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("tag_length", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("anchor", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("contributor", "STRING", mode="REPEATED"),
    bigquery.SchemaField("biosample", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("sra_experiment", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "contact",
        "RECORD",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField("phone", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("institute", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("web_link", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("department", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                "name",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("last", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("middle", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("first", "STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("zip_postal_code", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("supplemental_files", "STRING", mode="REPEATED"),
    bigquery.SchemaField("scan_protocol", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("tag_count", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("hyb_protocol", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("accession", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("last_update_date", "DATE", mode="NULLABLE"),
]


schema["gpl"] = [
    bigquery.SchemaField("manufacture_protocol", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("relation", "STRING", mode="REPEATED"),
    bigquery.SchemaField("data_row_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("manufacturer", "STRING", mode="REPEATED"),
    bigquery.SchemaField("distribution", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("series_id", "STRING", mode="REPEATED"),
    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("sample_id", "STRING", mode="REPEATED"),
    bigquery.SchemaField("summary", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "contact",
        "RECORD",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField("phone", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("institute", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("web_link", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("department", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                "name",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("last", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("middle", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("first", "STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("zip_postal_code", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("technology", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("accession", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("contributor", "JSON", mode="NULLABLE"),
    bigquery.SchemaField("last_update_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("submission_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("organism", "STRING", mode="NULLABLE"),
]


def get_schema(entity: str):
    """Get the schema for the given entity.

    Args:
        entity (str): The entity to get the schema for. One of 'gse', 'gsm', 'gpl'.
    """
    return schema[entity]
