import pandas
from prefect import flow, get_run_logger
from google.cloud import bigquery
import re
from pathlib import Path

PROJECT_ID = "omicidx-338300"
DATASET_ID = "biodatalake"
TABLE_ID = "src_scimago__journal_impact_factors"


@flow
def ingest_scimago_flow() -> None:
    get_run_logger().info("Ingesting Scimago Journal Impact Factors")
    scimago = pandas.read_csv(
        "https://www.scimagojr.com/journalrank.php?out=xls", delimiter=";"
    )

    scimago.rename(
        lambda x: re.sub(r"[^\w\d_]+", "_", x.lower()).strip("_"),
        axis="columns",
        inplace=True,
    )

    get_run_logger().info("Ingested Scimago Journal Impact Factors")
    scimago.to_json("scimago.ndjson.gz", orient="records", lines=True)
    get_run_logger().info("Saved Scimago Journal Impact Factors to ndjson.gz")
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        source_format="NEWLINE_DELIMITED_JSON",
    )
    client = bigquery.Client()
    res = client.load_table_from_file(
        open("scimago.ndjson.gz", "rb"),
        destination=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}",
        job_config=job_config,
    )
    get_run_logger().info(f"Loaded Scimago to BigQuery table {res.table_id}")
    get_run_logger().info(res.result())
    Path("scimago.ndjson.gz").unlink()


if __name__ == "__main__":
    ingest_scimago_flow()
